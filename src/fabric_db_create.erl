% Copyright 2010 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_db_create).
-export([go/2]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(DBNAME_REGEX, "^[a-z][a-z0-9\\_\\$()\\+\\-\\/]*$").

%% @doc Create a new database, and all its partition files across the cluster
%%      Options is proplist with user_ctx, n, q, validate_name
go(DbName, Options) ->
    case validate_dbname(DbName, Options) of
    ok ->
        {MegaSecs, Secs, _} = now(),
        Suffix = "." ++ integer_to_list(MegaSecs*1000000 + Secs),
        Shards = mem3:choose_shards(DbName, [{shard_suffix,Suffix} | Options]),
        case mem3_util:open_db_doc(DbName) of
        {ok, Doc} ->
            % the DB already exists, and may have a different Suffix
            ok;
        {not_found, _} ->
            Doc = make_document(Shards, Suffix)
        end,
        Workers = fabric_util:submit_jobs(Shards, create_db, []),
        Acc0 = fabric_dict:init(Workers, nil),

        W = list_to_integer(couch_util:get_value(w,Options,"2")),

        % create references to monitor
        RexiMon = fabric_util:create_monitors(Shards),

        X = fabric_util:recv(Workers, #shard.ref, fun handle_message/3, {length(Workers), W, Acc0}),

        rexi_monitor:stop(RexiMon),

        case update_shard_db(Doc) of
        {ok, true} ->
            case X of {ok, ok} -> ok;
            {ok, part_ok} -> part_ok;
            Else -> Else
            end;
        {ok, false} ->
            {error, internal_server_error}
        end;
    Error ->
        Error
    end.

validate_dbname(DbName, Options) ->
    case couch_util:get_value(validate_name, Options, true) of
    false ->
        ok;
    true ->
        case re:run(DbName, ?DBNAME_REGEX, [{capture,none}]) of
        match ->
            ok;
        nomatch when DbName =:= <<"_users">> ->
            ok;
        nomatch when DbName =:= <<"_replicator">> ->
            ok;
        nomatch ->
            {error, illegal_database_name}
        end
    end.

handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Shard, {WorkerLen, W, Counters}) ->
    NewCounters =
        fabric_dict:filter(fun(#shard{node=Node}, _) ->
                                Node =/= NodeRef
                       end, Counters),
    maybe_answer(WorkerLen - (fabric_dict:size(Counters) - fabric_dict:size(NewCounters)),
          W, NewCounters);

handle_message(Msg, Shard, {WorkerLen, W, Counters}) ->
    C1 = fabric_dict:store(Shard, Msg, Counters),
    maybe_answer(WorkerLen-1, W, C1).

update_shard_db(Doc) ->
    Shards = [#shard{node=N} || N <- mem3:nodes()],
    RexiMon = fabric_util:create_monitors(Shards),
    Workers = fabric_util:submit_jobs(Shards, create_shard_db_doc, [Doc]),
    Acc0 = fabric_dict:init(Workers, nil),
    try
        fabric_util:recv(Workers, #shard.ref, fun handle_db_update/3, Acc0)
    after
        rexi_monitor:stop(RexiMon)
    end.

handle_db_update({rexi_DOWN, _, {_,NodeRef},_}, _Worker, Counters) ->
    {ok,
     fabric_dict:filter(fun(#shard{node=Node}, _) ->
                                Node =/= NodeRef
                            end, Counters)};
handle_db_update({ok, _}, Worker, Counters) ->
    handle_db_update(ok, Worker, Counters);
handle_db_update(Msg, Worker, Counters) ->
    C1 = fabric_dict:store(Worker, Msg, Counters),
    case fabric_dict:any(nil, C1) of
    true ->
        {ok, C1};
    false ->
        {stop, fabric_dict:any(ok, C1)}
    end.

make_document([#shard{dbname=DbName}|_] = Shards, Suffix) ->
    {RawOut, ByNodeOut, ByRangeOut} =
    lists:foldl(fun(#shard{node=N, range=[B,E]}, {Raw, ByNode, ByRange}) ->
        Range = ?l2b([couch_util:to_hex(<<B:32/integer>>), "-",
            couch_util:to_hex(<<E:32/integer>>)]),
        Node = couch_util:to_binary(N),
        {[[<<"add">>, Range, Node] | Raw], orddict:append(Node, Range, ByNode),
            orddict:append(Range, Node, ByRange)}
    end, {[], [], []}, Shards),
    #doc{id=DbName, body = {[
        {<<"shard_suffix">>, Suffix},
        {<<"changelog">>, lists:sort(RawOut)},
        {<<"by_node">>, {[{K,lists:sort(V)} || {K,V} <- ByNodeOut]}},
        {<<"by_range">>, {[{K,lists:sort(V)} || {K,V} <- ByRangeOut]}}
    ]}}.

maybe_answer(WorkerLen, W, Counters) ->
    case fabric_view:is_progress_possible(Counters) of
    true ->
        case lists:keymember(file_exists, 2, Counters) of
        true ->
            {error, file_exists};
        false ->
            QuorumMet = fabric_util:quorum_met(W, Counters),
            case QuorumMet of
            true ->
                {stop, ok};
            false ->
                if WorkerLen > 0 ->
                    {ok, {WorkerLen, W, Counters}};
                   true ->
                    {stop, part_ok}
                end
            end
        end;
    false ->
        {error, internal_server_error}
    end.

db_create_ok_test() ->
    Shards = mem3_util:create_partition_map("foo",3,12,["node1","node2","node3"]),
    Acc0 = fabric_dict:init(Shards, nil),
    WorkerLen = length(Shards),
    Result = lists:foldl(fun(Shard,{Acc,_}) ->
                        case handle_message(ok,Shard,Acc) of
                            {ok, NewAcc} ->
                                {NewAcc,true};
                            {stop, ok} -> {Acc,true};
                            {error, _} -> {Acc, false}
                        end end, {{WorkerLen, 3, Acc0}, true}, Shards),
    ?assertEqual(element(2,Result), true).

db_create_file_exists_test() ->
    Shards = mem3_util:create_partition_map("foo",3,12,["node1","node2","node3","node4","node5"]),
    BadNo = random:uniform(length(Shards)),
    Acc0 = fabric_dict:init(Shards, nil),
    WorkerLen = length(Shards),
    Result = lists:foldl(
               fun(Shard,{Acc,Iter,Bool}) ->
                       MessResult = case Iter of
                                        BadNo ->
                                            handle_message(file_exists,Shard,Acc);
                                        _ ->
                                            handle_message(ok,Shard,Acc)
                                    end,
                       case MessResult of
                           {ok, NewAcc} ->
                               {NewAcc, Iter+1, Bool};
                           {stop, ok} -> {Acc, Iter+1, Bool};
                           {error, _} -> {Acc, Iter+1, false}
                       end
               end,{{WorkerLen, 3, Acc0}, 1, true}, Shards),
    ?assertEqual(element(3,Result),false).
