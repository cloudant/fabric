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

-module(fabric_util).

-export([submit_jobs/3, cleanup/1, recv/4, get_db/1, get_db/2, error_info/1,
        update_counter/3, remove_ancestors/2, create_monitors/1, quorum_met/2,
        kv/2]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").

submit_jobs(Shards, EndPoint, ExtraArgs) ->
    lists:map(fun(#shard{node=Node, name=ShardName} = Shard) ->
        Ref = rexi:cast(Node, {fabric_rpc, EndPoint, [ShardName | ExtraArgs]}),
        Shard#shard{ref = Ref}
    end, Shards).

cleanup(Workers) ->
    [rexi:kill(Node, Ref) || #shard{node=Node, ref=Ref} <- Workers].

recv(Workers, Keypos, Fun, Acc0) ->
    Timeout = case couch_config:get("fabric", "request_timeout", "60000") of
    "infinity" -> infinity;
    N -> list_to_integer(N)
    end,
    rexi_utils:recv(Workers, Keypos, Fun, Acc0, Timeout, infinity).


get_db(DbName) ->
    get_db(DbName, []).

get_db(DbName, Options) ->
    % prefer local shards
    {Local, Remote} = lists:partition(fun(S) -> S#shard.node =:= node() end,
        mem3:shards(DbName)),
    % suppress shards from down nodes
    Nodes = erlang:nodes(),
    Live = [S || #shard{node = N} = S <- Remote, lists:member(N, Nodes)],
    % sort the live remote shards so that we don't repeatedly try the same node
    get_shard(Local ++ lists:keysort(#shard.name, Live), Options, 100).

get_shard([], _Opts, _Timeout) ->
    erlang:error({internal_server_error, "No DB shards could be opened."});
get_shard([#shard{node = Node, name = Name} | Rest], Opts, Timeout) ->
    case rpc:call(Node, couch_db, open, [Name, [{timeout, Timeout} | Opts]]) of
    {ok, Db} ->
        {ok, Db};
    {unauthorized, _} = Error ->
        throw(Error);
    {badrpc, {'EXIT', {timeout, _}}} ->
        get_shard(Rest, Opts, 2*Timeout);
    _Else ->
        get_shard(Rest, Opts, Timeout)
    end.

error_info({{<<"reduce_overflow_error">>, _} = Error, _Stack}) ->
    Error;
error_info({{timeout, _} = Error, _Stack}) ->
    Error;
error_info({{Error, Reason}, Stack}) ->
    {Error, Reason, Stack};
error_info({Error, Stack}) ->
    {Error, nil, Stack}.

update_counter(Item, Incr, D) ->
    UpdateFun = fun ({Old, Count}) -> {Old, Count + Incr} end,
    orddict:update(make_key(Item), UpdateFun, {Item, Incr}, D).

make_key({ok, L}) when is_list(L) ->
    make_key(L);
make_key([]) ->
    [];
make_key([{ok, #doc{revs= {Pos,[RevId | _]}}} | Rest]) ->
    [{ok, {Pos, RevId}} | make_key(Rest)];
make_key([{{not_found, missing}, Rev} | Rest]) ->
    [{not_found, Rev} | make_key(Rest)];
make_key({ok, #doc{id=Id,revs=Revs}}) ->
    {Id, Revs};
make_key(Else) ->
    Else.

% this presumes the incoming list is sorted, i.e. shorter revlists come first
remove_ancestors([], Acc) ->
    lists:reverse(Acc);
remove_ancestors([{_, {{not_found, _}, Count}} = Head | Tail], Acc) ->
    % any document is a descendant
    case lists:filter(fun({_,{{ok, #doc{}}, _}}) -> true; (_) -> false end, Tail) of
    [{_,{{ok, #doc{}} = Descendant, _}} | _] ->
        remove_ancestors(update_counter(Descendant, Count, Tail), Acc);
    [] ->
        remove_ancestors(Tail, [Head | Acc])
    end;
remove_ancestors([{_,{{ok, #doc{revs = {Pos, Revs}}}, Count}} = Head | Tail], Acc) ->
    Descendants = lists:dropwhile(fun
    ({_,{{ok, #doc{revs = {Pos2, Revs2}}}, _}}) ->
        case lists:nthtail(erlang:min(Pos2 - Pos, length(Revs2)), Revs2) of
        [] ->
            % impossible to tell if Revs2 is a descendant - assume no
            true;
        History ->
            % if Revs2 is a descendant, History is a prefix of Revs
            not lists:prefix(History, Revs)
        end
    end, Tail),
    case Descendants of [] ->
        remove_ancestors(Tail, [Head | Acc]);
    [{Descendant, _} | _] ->
        remove_ancestors(update_counter(Descendant, Count, Tail), Acc)
    end.

create_monitors(Shards) ->
    MonRefs = lists:usort([{rexi_server, N} || #shard{node=N} <- Shards]),
    rexi_monitor:start(MonRefs).

quorum_met(W, Replies) ->
    Counters = lists:foldl(fun({#shard{name=Name},M},D) ->
                               if M == ok ->
                                   orddict:update_counter(Name,1,D);
                                  true ->
                                   D
                               end end, orddict:new(), Replies),
    % note: lists:all can be vacuously true
    orddict:size(Counters) > 0 andalso
    lists:all(fun({_,Count}) -> Count >= W end, Counters).

%% verify only id and rev are used in key.
update_counter_test() ->
    Reply = {ok, #doc{id = <<"id">>, revs = <<"rev">>,
                    body = <<"body">>, atts = <<"atts">>}},
    ?assertEqual([{{<<"id">>,<<"rev">>}, {Reply, 1}}],
        update_counter(Reply, 1, [])).

remove_ancestors_test() ->
    Foo1 = {ok, #doc{revs = {1, [<<"foo">>]}}},
    Foo2 = {ok, #doc{revs = {2, [<<"foo2">>, <<"foo">>]}}},
    Bar1 = {ok, #doc{revs = {1, [<<"bar">>]}}},
    Bar2 = {not_found, {1,<<"bar">>}},
    ?assertEqual(
        [kv(Bar1,1), kv(Foo1,1)],
        remove_ancestors([kv(Bar1,1), kv(Foo1,1)], [])
    ),
    ?assertEqual(
        [kv(Bar1,1), kv(Foo2,2)],
        remove_ancestors([kv(Bar1,1), kv(Foo1,1), kv(Foo2,1)], [])
    ),
    ?assertEqual(
        [kv(Bar1,2)],
        remove_ancestors([kv(Bar2,1), kv(Bar1,1)], [])
    ).

%% test function
kv(Item, Count) ->
    {make_key(Item), {Item,Count}}.
