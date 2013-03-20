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

-export([submit_jobs/3, submit_jobs/4, cleanup/1, recv/4, get_db/1, get_db/2, error_info/1,
        update_counter/3, remove_ancestors/2, create_monitors/1, kv/2,
        update_seqs/3,
        remove_down_workers/2, remove_non_preferred_shards/2,
        pack/1, process_enc_shards/2]).
-export([request_timeout/0]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").

remove_down_workers(Workers, BadNode) ->
    Filter = fun(#shard{node = Node}, _) -> Node =/= BadNode end,
    NewWorkers = fabric_dict:filter(Filter, Workers),
    case fabric_view:is_progress_possible(NewWorkers) of
    true ->
        {ok, NewWorkers};
    false ->
        error
    end.

submit_jobs(Shards, EndPoint, ExtraArgs) ->
    submit_jobs(Shards, fabric_rpc, EndPoint, ExtraArgs).

submit_jobs(Shards, Module, EndPoint, ExtraArgs) ->
    lists:map(fun(#shard{node=Node, name=ShardName} = Shard) ->
        Ref = rexi:cast(Node, {Module, EndPoint, [ShardName | ExtraArgs]}),
        Shard#shard{ref = Ref}
    end, Shards).

cleanup(Workers) ->
    [rexi:kill(Node, Ref) || #shard{node=Node, ref=Ref} <- Workers].

recv(Workers, Keypos, Fun, Acc0) ->
    rexi_utils:recv(Workers, Keypos, Fun, Acc0, request_timeout(), infinity).

request_timeout() ->
    case config:get("fabric", "request_timeout", "60000") of
        "infinity" -> infinity;
        N -> list_to_integer(N)
    end.

get_db(DbName) ->
    get_db(DbName, []).

get_db(DbName, Options) ->
    {Local, SameZone, DifferentZone} = mem3:group_by_proximity(mem3:shards(DbName)),
    % Prefer shards on the same node over other nodes, prefer shards in the same zone over
    % over zones and sort each remote list by name so that we don't repeatedly try the same node.
    Shards = Local ++ lists:keysort(#shard.name, SameZone) ++ lists:keysort(#shard.name, DifferentZone),
    % suppress shards from down nodes
    Nodes = [node()|erlang:nodes()],
    Live = [S || #shard{node = N} = S <- Shards, lists:member(N, Nodes)],
    get_shard(Live, Options, 100).

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
    end;
remove_ancestors([Error | Tail], Acc) ->
    remove_ancestors(Tail, [Error | Acc]).

create_monitors(Shards) ->
    MonRefs = lists:usort([{rexi_server, N} || #shard{node=N} <- Shards]),
    rexi_monitor:start(MonRefs).

remove_non_preferred_shards(PreferredShards, Shards) ->
    %% the preferred shards are the good ones. Remove shards that
    %% are in the same range but different nodes
    lists:foldl(fun(Shard, Acc) ->
                    case check_shard(Shard, PreferredShards) of
                    -1 ->
                        Acc;
                    DbSeq ->
                        [{Shard, DbSeq} | Acc]
                    end
                end, [], Shards).

check_shard(_, []) ->
    0;
check_shard(#shard{name=Name, node=Node}=Shard, [{#shard{name=Name2, node=Node2}, DbSeq} | Rest]) ->
    case Name =:= Name2 of
    true ->
        case Node =:= Node2 of
        true ->
            DbSeq;
        false ->
            -1
        end;
    _ ->
        check_shard(Shard, Rest)
    end.


process_enc_shards(EncShards, DbName) ->
    case EncShards of
    [] -> [];
    _ ->
        unpack_seqs(EncShards, DbName)
    end.

pack(Responders) ->
    ShardList = [{N,R,DbSeq} || {#shard{node=N, range=R}, DbSeq} <- Responders],
    couch_util:encodeBase64Url(term_to_binary(ShardList, [compressed])).

unpack_seqs(Packed, DbName) ->
    NodeRangeSeqs = unpack_and_merge(Packed),
    lists:map(fun({Node, Range, DbSeq}) ->
                  {ok, Shard} = mem3:get_shard(DbName, Node, Range),
                  {Shard, DbSeq}
              end,
              NodeRangeSeqs).

unpack_and_merge(PackedNRS) ->
    NodeRangeSeqs =
        lists:foldl(fun(NRQ, Acc) ->
                        binary_to_term(couch_util:decodeBase64Url(NRQ)) ++ Acc
                    end,[],PackedNRS),
    MergedList =
        lists:foldl(fun({Node,Range,Seq}, Acc) ->
                        [merge({Node,Range,Seq}, NodeRangeSeqs) | Acc]
                    end,[],NodeRangeSeqs),
    lists:usort(MergedList).

merge(NRS, []) ->
    NRS;

merge({Node, Range, Seq}, [{Node2, Range2, Seq2} | Rest]) ->
    case Node =:= Node2 andalso Range =:= Range2 of
    true ->
        {Node, Range, erlang:max(Seq,Seq2)};
    false ->
        merge({Node, Range, Seq}, Rest)
    end.

update_seqs(DbSeq, Worker, #collector{seqs=Seqs}=State) ->
    case lists:member({Worker, DbSeq}, Seqs) of
    true ->
        NewSeqs = Seqs;
    false ->
        NewSeqs = [{Worker, DbSeq} | Seqs]
    end,
    State#collector{seqs=NewSeqs}.

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

remove_non_preferred_shards_test() ->
    Shards =
        [{shard,<<"shards/00000000-7fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/80000000-ffffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [536870912,1073741823],
          undefined},
         {shard,<<"shards/00000000-7fffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/80000000-ffffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [536870912,1073741823],
          undefined},
         {shard,<<"shards/00000000-7fffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/80000000-ffffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [536870912,1073741823],
          undefined}],
    PreferredShards =
        [{{shard,<<"shards/00000000-7fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [0,536870911],
          undefined},2},
         {{shard,<<"shards/80000000-ffffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [536870912,1073741823],
           undefined},3}],
    NewShards = remove_non_preferred_shards(PreferredShards, Shards),
    ?assertEqual(lists:reverse(NewShards),PreferredShards),
    ok.

remove_non_preferred_shards2_test() ->
    Shards =
        [{shard,<<"shards/00000000-3fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},
         {shard,<<"shards/80000000-bfffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [2147483648,2684354559],
          undefined},
         {shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},
         {shard,<<"shards/00000000-3fffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},
         {shard,<<"shards/80000000-bfffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [2147483648,2684354559],
          undefined},
         {shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},
         {shard,<<"shards/00000000-3fffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},
         {shard,<<"shards/80000000-bfffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [2147483648,2684354559],
          undefined},
         {shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [3221225472,3758096383],
          undefined}],
    PreferredShards =
        [{{shard,<<"shards/00000000-3fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [0,536870911],
          undefined},2},
         {{shard,<<"shards/80000000-bfffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [536870912,1073741823],
           undefined},3}],
    ExpectedShards =
            [{{shard,<<"shards/00000000-3fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [0,536870911],
          undefined},2},
         {{shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},0},
         {{shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node1',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},0},
         {{shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},0},
         {{shard,<<"shards/80000000-bfffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [2147483648,2684354559],
          undefined},3},
         {{shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node2',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},0},
         {{shard,<<"shards/40000000-7fffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},0},
         {{shard,<<"shards/c0000000-ffffffff/testdb1">>,
          'node3',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},0}],
    NewShards = remove_non_preferred_shards(PreferredShards, Shards),
    ?assertEqual(lists:reverse(NewShards),ExpectedShards),
    ok.

unpack_and_merge_test() ->
    Shards =
        [{shard,<<"shards/00000000-1fffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [0,536870911],
          undefined},
         {shard,<<"shards/20000000-3fffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [536870912,1073741823],
          undefined},
         {shard,<<"shards/40000000-5fffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [1073741824,1610612735],
          undefined},
         {shard,<<"shards/60000000-7fffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [1610612736,2147483647],
          undefined},
         {shard,<<"shards/80000000-9fffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [2147483648,2684354559],
          undefined},
         {shard,<<"shards/a0000000-bfffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [2684354560,3221225471],
          undefined},
         {shard,<<"shards/c0000000-dfffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [3221225472,3758096383],
          undefined},
         {shard,<<"shards/e0000000-ffffffff/testdb1">>,
          'bigcouch@node.local',<<"testdb1">>,
          [3758096384,4294967295],
          undefined}],
    Responders1 = lists:map(fun(S) ->
                                {S, 1}
                            end, Shards),
    Responders2 = lists:map(fun(S) ->
                                {S, 2}
                            end, Shards),
    P1 = pack(Responders1),
    P2 = pack(Responders2),
    ShardList = [{N,R,DbSeq} || {#shard{node=N, range=R}, DbSeq} <- Responders2],
    ?assertEqual(ShardList, unpack_and_merge([P1, P2])),
    ok.

