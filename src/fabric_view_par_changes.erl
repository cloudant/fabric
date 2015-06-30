% Copyright 2015 Cloudant
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

-module(fabric_view_par_changes).

-export([get_url/5, direct/7, keep_direct_changes/8]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-record (dcacc, {
    db,
    seq,
    args,
    options,
    callback,
    limit,
    pending,
    respacc
}).

get_url(DbName, _, #changes_args{parallel=true, since=OrigSeq}=Options,
        Callback, Acc) ->
    Args = fabric_view_changes:make_changes_args(Options),
    Since = fabric_view_changes:get_start_seq(DbName, Args),
    case fabric_view_changes:validate_start_seq(DbName, Since) of
        ok ->
            ShardsInfo = get_shards_info_list(DbName, Since),
            Callback({shard_info, ShardsInfo, OrigSeq}, Acc);
        Error ->
            Callback(Error, Acc)
    end.

direct(DbName, "normal", Node, Range, Options, Callback, Acc) ->
    SameNode = Node =:= get_node_name(),
    Args = fabric_view_changes:make_changes_args(Options),
    Since = fabric_view_changes:get_start_seq(DbName, Args),
    SinceInt = (catch list_to_integer(Since)),
    case {SameNode, is_integer(SinceInt)} of
        {true, true} ->
            {ok, RespAcc} = Callback(start, Acc),
            {ok, #dcacc{seq=LastSeq, respacc=RespAccOut,
                pending=Pending}} = direct_changes(
                    DbName,
                    Range,
                    Args,
                    Callback,
                    SinceInt,
                    RespAcc
                ),
        Callback({stop, LastSeq, Pending}, RespAccOut);
    {true, false} ->
        Reason = <<"Seq number must be an int for direct changes.">>,
        Callback({error, {bad_request, Reason}}, Acc);
    {false, _} ->
        Reason = <<"Node argument does not match this node.">>, 
        Callback({error, {not_found, Reason}}, Acc)
    end;
direct(DbName, Feed, Node, Range, Options, Callback, Acc)
        when Feed == "continuous" orelse Feed == "longpoll" ->
    SameNode = Node =:= get_node_name(),
    Args = fabric_view_changes:make_changes_args(Options),
    Since = fabric_view_changes:get_start_seq(DbName, Args),
    SinceInt = (catch list_to_integer(Since)),
    case {SameNode, is_integer(SinceInt)} of
        {true, true} ->
            {ok, RespAcc} = Callback(start, Acc),
            {Timeout, _} = couch_changes:get_changes_timeout(Args, Callback),
            Ref = make_ref(),
            Parent = self(),
            UpdateListener = {spawn_link(fabric_db_update_listener, go,
                [Parent, Ref, DbName, Timeout]),Ref},
            put(changes_epoch, fabric_view_changes:get_changes_epoch()),
            try
                keep_direct_changes(
                    DbName,
                    Range,
                    Args,
                    Callback,
                    SinceInt,
                    RespAcc,
                    UpdateListener,
                    os:timestamp()
                )
            after
                fabric_db_update_listener:stop(UpdateListener)
            end;
        {true, false} ->
            Reason = <<"Seq number must be an int for direct changes.">>,
            Callback({error, {bad_request, Reason}}, Acc);
        {false, _} ->
            Reason = <<"Node argument does not match this node.">>,
            Callback({error, {not_found, Reason}}, Acc)
    end.

direct_changes(DbName, Range, ChangesArgs, Callback, StartSeq, AccIn) ->
    % TODO: Failure case, when shard range is down or incorrect
    Suffix = mem3:shard_suffix(DbName),
    ShardPath0 = ["shards/", Range, "/", DbName, Suffix],
    ShardPath = list_to_binary(lists:flatten(ShardPath0)),
    #changes_args{dir=Dir} = ChangesArgs,
    {ok, Db} = couch_db:open_int(ShardPath, []),
    Enum = fun direct_changes_enumerator/2,
    Opts = [{dir,Dir}],
    try
        ChangeAcc = #dcacc{db=Db,
            seq=StartSeq,
            args=ChangesArgs,
            options=[ChangesArgs],
            callback=Callback,
            limit = ChangesArgs#changes_args.limit,
            pending = couch_db:count_changes_since(Db, StartSeq),
            respacc=AccIn},
        couch_db:changes_since(Db, StartSeq, Enum, Opts, ChangeAcc)
    after
        couch_db:close(Db)
    end.

keep_direct_changes(DbName, Range, Args0, Callback, Since, AccIn,
        UpListen, T0) ->
    #changes_args{limit=Limit, feed=Feed, heartbeat=Heartbeat} = Args0,
    {ok, #dcacc{seq=LastSeq, respacc=RespAccOut,
        args=Args}} = direct_changes(
            DbName,
            Range,
            Args0,
            Callback,
            Since,
            AccIn
        ),
    #changes_args{limit=Limit2} = Args,
    MaintenanceMode = config:get("cloudant", "maintenance_mode"),
    NewEpoch = fabric_view_changes:get_changes_epoch() > erlang:get(changes_epoch),
    if Limit > Limit2, Feed == "longpoll";
      MaintenanceMode == "true"; MaintenanceMode == "nolb"; NewEpoch ->
        Callback({stop, LastSeq, 0}, RespAccOut);
    true ->
        WaitForUpdate = fabric_db_update_listener:wait_db_updated(UpListen),
        AccumulatedTime = timer:now_diff(os:timestamp(), T0) div 1000,
        Max = case config:get("fabric", "changes_duration") of
        undefined ->
            infinity;
        MaxStr ->
            list_to_integer(MaxStr)
        end,
        case {Heartbeat, AccumulatedTime > Max, WaitForUpdate} of
        {undefined, _, timeout} ->
            Callback({stop, LastSeq, 0}, RespAccOut);
        {_, true, timeout} ->
            Callback({stop, LastSeq, 0}, RespAccOut);
        _ ->
            {ok, AccTimeout} = Callback(timeout, RespAccOut),
            ?MODULE:keep_direct_changes(
                DbName,
                Range,
                Args#changes_args{limit=Limit2},
                Callback,
                LastSeq,
                AccTimeout,
                UpListen,
                T0
            )
        end
    end.

% Returns a balanced list of nodes for the entire shard range.
get_shards_info_list(DbName, PackedSeqs) ->
    LiveNodes = [node() | nodes()],
    AllLiveShards = mem3:live_shards(DbName, LiveNodes),
    Shards = lists:flatmap(fun({Shard, Seq}) ->
        case lists:member(Shard, AllLiveShards) of
            true ->
                [{Shard, Seq}];
            false ->
                NewShard = get_replacement_shard(Shard, AllLiveShards),
                [{NewShard, Seq}]
        end
    end, fabric_view_changes:unpack_seqs(PackedSeqs, DbName)),
    balance_shards_list(Shards).

balance_shards_list(Shards) ->
    {SInfo, _} = lists:foldl(fun({Shard, Seq}, {SInfo0, NodeCount0}) ->
        #shard{node=Node, dbname=DbName, range=Range} = Shard,
        case lists:keyfind(Range, 2, SInfo0) of
            false ->
                SInfo1 = [{Node, Range, DbName, Seq} | SInfo0],
                NodeCount = orddict:update_counter(Node, 1, NodeCount0),
                {SInfo1, NodeCount};
            {ExistNode, _, _ , _} when ExistNode =/= Node ->
                maybe_replace_shard_info(SInfo0, NodeCount0, ExistNode,
                    Node, Range, DbName, Seq)
        end
    end, {[], orddict:new()}, Shards),
    SInfo.

maybe_replace_shard_info(SInfo0, NodeCount0, ExistNode, Node, Range,
        DbName, Seq) ->
    ExistVal = orddict:find(ExistNode, NodeCount0),
    NewVal = orddict:find(Node, NodeCount0),
    case {ExistVal, NewVal} of
        {{ok, E}, {ok, N}} when E > 1, E > N ->
            replace_shard_info(SInfo0, NodeCount0, ExistNode, Node,
                Range,DbName, Seq);
        {{ok, E}, error}  when E > 1 ->
            replace_shard_info(SInfo0, NodeCount0, ExistNode, Node,
                Range,DbName, Seq);
        _ ->
            {SInfo0, NodeCount0}
    end.

replace_shard_info(SInfo0, NodeCount0, ExistNode, Node, Range,
        DbName, Seq)->
    SInfo = lists:keyreplace(ExistNode, 1, SInfo0, {Node, Range,
                DbName, Seq}),
    NodeCount1 = orddict:update_counter(ExistNode, -1, NodeCount0),
    NodeCount = orddict:update_counter(Node, 1, NodeCount1),
    {SInfo, NodeCount}.

direct_changes_enumerator(#doc_info{id= <<"_local/", _/binary>>,
        high_seq=Seq}, Dcacc) ->
    {ok, Dcacc#dcacc{seq = Seq}};
direct_changes_enumerator(_DocInfo, #dcacc{limit=0} = Dcacc) ->
    {stop, Dcacc};
direct_changes_enumerator(DocInfo, Dcacc) ->
    #dcacc{
        db = Db,
        args = #changes_args{include_docs = IncludeDocs, filter = Filter},
        options = Options,
        callback = Callback,
        limit=Limit,
        pending = Pending,
        respacc = RespAcc0
    } = Dcacc,
    Conflicts = proplists:get_value(conflicts, Options, false),
    #doc_info{id=Id, high_seq=Seq, revs=[#rev_info{deleted=Del}|_]} = DocInfo,
    case [X || X <- couch_changes:filter(DocInfo, Filter), X /= null] of
        [] ->
            ChangesRow = {no_pass, Seq};
        Results ->
            Opts = if Conflicts -> [conflicts]; true -> [] end,
            ChangesRow = [
                {seq, Seq},
                {id, Id},
                {changes, Results},
                {deleted, Del} |
                if IncludeDocs -> [doc_member(Db, DocInfo, Opts)]; 
                    true -> [] end
            ]
    end,
    {ok, RespAcc} = Callback(fabric_view_changes:changes_row(ChangesRow),
        RespAcc0),
    {ok, Dcacc#dcacc{seq = Seq, limit = Limit-1, pending = Pending-1,
        respacc=RespAcc}}.

get_replacement_shard(Shard, AllLiveShards) ->
    Shards = fabric_view_changes:find_replacement_shards(Shard,
        AllLiveShards),
    N = random:uniform(length(Shards)),
    lists:nth(N, Shards).

get_node_name() ->
    [NodeName | _] = string:tokens(atom_to_list(node()), "@"),
    list_to_binary(NodeName).

doc_member(Shard, DocInfo, Opts) ->
    case couch_db:open_doc(Shard, DocInfo, [deleted | Opts]) of
    {ok, Doc} ->
        {doc, couch_doc:to_json_obj(Doc, [])};
    Error ->
        Error
    end.
