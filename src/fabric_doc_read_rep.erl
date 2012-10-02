% Copyright 2012 Cloudant
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

-module(fabric_doc_read_rep).

-export([go/3]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").


go(DbName, Id, Options) ->
    Shards = mem3:shards(DbName, Id),
    {ok, Trees} = fabric:open_fdi(DbName, Id, [{r, N} | Options]),
    RevsDiff = revs_diff(Trees),
    JobsByShard = group_revs_diff(RevsDiff),
    Workers = lists:foldl(fun({#shard{node=Node}=Shard, Revs}, WAcc) ->
        Args = [DbName Id, Revs, Options],
        Ref = rexi:cast(Node, {fabric_rpc, repair_doc, Args}),
        [Shard#shard{ref=Ref} | WAcc]
    end, [], JobsByShard),
    RexiMon = fabric_util:create_monitors(Workers),
    try fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Workers) of
    {ok, ok} ->
        ok;
    Error ->
        Error
    after
        rexi_monitor:stop(RexiMon)
    end.


handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Worker, Workers) ->
    NewWorkers = lists:keydelete(NodeRef, #shard.node, Workers),
    maybe_stop(NewWorkers);
handle_message(_, Worker, Workers) ->
    NewWorkers = lists:delete(Worker, Workers),
    maybe_stop(NewWorkers).


maybe_stop([]) ->
    {stop, ok};
maybe_stop(Workers) ->
    {ok, Workers}.


revs_diff(Trees) ->
    revs_diff(Trees, Trees, []).


revs_diff([], _Trees, Acc) ->
    Acc;
revs_diff([{#shard{}=W, #full_doc_info{}=FDI} | Rest], Trees, Acc) ->
    revs_diff(W, FDI, Trees, Acc);
revs_diff([{#shard{}, _Error} | Rest], Trees, Acc) ->
    revs_diff(Rest, Trees, Acc).


revs_diff(W, FDI, [], Acc) ->
    Acc;
revs_diff(W, FDI, Trees, Acc) ->
    Leafs0 = couch_key_tree:get_all_leafs(FDI#full_doc_info.rev_tree),
    Leafs = [{Pos, Key} || {_Value, {Pos, [Key | _]}} <- Leafs0],
    find_missing(W, Leafs, Trees, Acc).


find_missing(_W, _Leafs, [], Acc) ->
    Acc;
find_missing(W, Leafs, [{W, _} | Rest], Acc) ->
    find_missing(W, Leafs, Rest, Acc);
find_missing(W, Leafs, [{_, #full_doc_info{}=FDI} | Rest], Acc0) ->
    Missing = couch_key_tree:find_missing(FDI#full_doc_info.rev_tree, Leafs),
    Acc = [{W, Leaf} || Leaf <- Missing] ++ Acc0;
    find_missing(W, Leafs, Rest, Acc);
find_missing(W, Leafs, [{_, _} | Rest], Acc0)
    Acc = [{W, Leaf} || Leaf <- Leafs] ++ Acc0,
    find_missing(W, Leafs, Rest, Acc).


group_revs_diff(RevsDiff) ->
    ByShardRev = lists:foldl(fun({W, R}, Acc0) ->
        dict:append(W, R, Acc0)
    end, dict:new(), RevsDiff),
    dict:to_list(ByShardRev).
