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

-module(fabric_doc_update).

-export([go/3]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(_, [], _) ->
    {ok, []};
go(DbName, AllDocs, Opts) ->
    validate_atomic_update(DbName, AllDocs, lists:member(all_or_nothing, Opts)),
    Options = lists:delete(all_or_nothing, Opts),
    GroupedDocs = lists:map(fun({#shard{name=Name, node=Node} = Shard, DocRefs}) ->
        Ref = rexi:cast(Node, {fabric_rpc, update_docs, [Name, extract_docs(DocRefs), Options]}),
        {Shard#shard{ref=Ref}, DocRefs}
    end, group_docs_by_shard(DbName, AllDocs)),
    {Workers, _} = lists:unzip(GroupedDocs),
    W = couch_util:get_value(w, Options, couch_config:get("cluster","w","2")),
    Acc0 = {length(Workers), list_to_integer(W), GroupedDocs, dict:new()},
    case fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0) of
    {ok, Results} ->
        Reordered = reorder_results(AllDocs, Results, []),
        {ok, [R || R <- Reordered, R =/= noreply]};
    Else ->
        Else
    end.

reorder_results([], [], Acc) ->
    lists:reverse(Acc);
reorder_results([Doc | RestDocs], Results, Acc) ->
    {value, {_, Val}, NewResults} = lists:keytake(Doc,1,Results),
    reorder_results(RestDocs,NewResults,[Val | Acc]).

handle_message({rexi_DOWN, _, _, _}, _Worker, Acc0) ->
    skip_message(Acc0);
handle_message({rexi_EXIT, _}, _Worker, Acc0) ->
    skip_message(Acc0);
handle_message(internal_server_error, _Worker, Acc0) ->
    % happens when we fail to load validation functions in an RPC worker
    skip_message(Acc0);
handle_message({ok, Replies}, Worker, Acc0) ->
    {WaitingCount, W, GroupedDocs, ReplyDict} = Acc0,
    DocRefs = couch_util:get_value(Worker, GroupedDocs),
    NewReplyDict = append_update_replies(DocRefs, Replies, ReplyDict),
    case WaitingCount of
    1 ->
        % last message has arrived, we need to conclude things
        {W, Reply} = dict:fold(fun force_reply/3, {W,[]}, NewReplyDict),
        {stop, Reply};
    _ ->
        case dict:fold(fun maybe_reply/3, {stop, W, []}, NewReplyDict) of
        continue ->
            {ok, {WaitingCount - 1, W, GroupedDocs, NewReplyDict}};
        {stop, W, FinalReplies} ->
            {stop, FinalReplies}
        end
    end;
handle_message({missing_stub, Stub}, _, _) ->
    throw({missing_stub, Stub});
handle_message({not_found, no_db_file} = X, Worker, Acc0) ->
    {_, _, GroupedDocs, _} = Acc0,
    DocRefs = couch_util:get_value(Worker, GroupedDocs),
    handle_message({ok, [X || _D <- DocRefs]}, Worker, Acc0).

force_reply({Doc, _Ref}, [], {W, Acc}) ->
    {W, [{Doc, {error, internal_server_error}} | Acc]};
force_reply({Doc, _Ref}, [FirstReply|_] = Replies, {W, Acc}) ->
    case update_quorum_met(W, Replies) of
    {true, Reply} ->
        {W, lists:foldl(fun(Elem, Acc1) ->
                          [{Doc, Elem} | Acc1]
                        end, Acc, Reply)};
    false ->
        twig:log(warn, "write quorum (~p) failed for ~s", [W, Doc#doc.id]),
        % TODO make a smarter choice than just picking the first reply
        {W, [{Doc,FirstReply} | Acc]}
    end.

maybe_reply(_, _, continue) ->
    % we didn't meet quorum for all docs, so we're fast-forwarding the fold
    continue;
maybe_reply({Doc, _Ref}, Replies, {stop, W, Acc}) ->
    case update_quorum_met(W, Replies) of
    {true, Reply} ->
        {stop, W, lists:foldl(fun(Elem, Acc1) ->
                                [{Doc, Elem} | Acc1]
                            end, Acc, Reply)};
    false ->
        continue
    end.

update_quorum_met(W, Replies) ->
    Counters = lists:foldl(fun(R,D) -> orddict:update_counter(R,1,D) end,
        orddict:new(), Replies),
    case lists:dropwhile(fun({_, Count}) -> Count < W end, Counters) of
    [] ->
        false;
    [{_, _} | _] = RedCounters ->
        {Results, _} = lists:unzip(RedCounters),
        {true, Results}
    end.

-spec group_docs_by_shard(binary(), [#doc{}]) -> [{#shard{}, [#doc{}]}].
group_docs_by_shard(DbName, Docs) ->
    dict:to_list(lists:foldl(fun(#doc{id=Id} = Doc, D0) ->
        % dups may occur, or docs with identical ids
        % create a reference to differentiate them
        Ref = make_ref(),
        lists:foldl(fun(Shard, D1) ->
            dict:append(Shard, {Doc, Ref}, D1)
        end, D0, mem3:shards(DbName,Id))
    end, dict:new(), Docs)).

extract_docs(DocRefs) ->
    element(1,lists:unzip(DocRefs)).


append_update_replies([], [], ReplyDict) ->
    ReplyDict;
append_update_replies([{Doc,Ref} | Rest], [], ReplyDict) ->
    append_update_replies(Rest, [], dict:append({Doc,Ref}, noreply, ReplyDict));
append_update_replies([{Doc, Ref}|Rest1], [Reply|Rest2], ReplyDict) ->
    append_update_replies(Rest1, Rest2, dict:append({Doc,Ref}, Reply, ReplyDict)).

skip_message({WaitingCount, W, _GroupedDocs, ReplyDict} = Acc0) ->
    if WaitingCount =:= 1 ->
        {W, Reply} = dict:fold(fun force_reply/3, {W,[]}, ReplyDict),
        {stop, Reply};
    true ->
        {ok, setelement(1, Acc0, WaitingCount-1)}
    end.

validate_atomic_update(_, _, false) ->
    ok;
validate_atomic_update(_DbName, AllDocs, true) ->
    % TODO actually perform the validation.  This requires some hackery, we need
    % to basically extract the prep_and_validate_updates function from couch_db
    % and only run that, without actually writing in case of a success.
    Error = {not_implemented, <<"all_or_nothing is not supported yet">>},
    PreCommitFailures = lists:map(fun(#doc{id=Id, revs = {Pos,Revs}}) ->
        case Revs of [] -> RevId = <<>>; [RevId|_] -> ok end,
        {{Id, {Pos, RevId}}, Error}
    end, AllDocs),
    throw({aborted, PreCommitFailures}).

% eunits
doc_update1_test() ->
    Doc1 = #doc{revs = {1,[<<"foo">>]}},
    Doc2 = #doc{revs = {1,[<<"bar">>]}},
    Docs = [Doc1],
    Docs2 = [Doc2, Doc1],
    Dict = dict:from_list([{Doc,[]} || Doc <- Docs]),
    Dict2 = dict:from_list([{Doc,[]} || Doc <- Docs2]),

    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedDocs = group_docs_by_shard_hack(<<"foo">>,Shards,Docs),


    % test for W = 2
    AccW2 = {length(Shards), length(Docs), list_to_integer("2"), GroupedDocs,
        Dict},

    {ok,{WaitingCountW2_1,_,_,_,_}=AccW2_1} =
        handle_message({ok, [{ok, Doc1}]},hd(Shards),AccW2),
    ?assertEqual(WaitingCountW2_1,2),
    {stop, FinalReplyW2 } =
        handle_message({ok, [{ok, Doc1}]},lists:nth(2,Shards),AccW2_1),
    ?assertEqual([{Doc1, {ok,Doc1}}],FinalReplyW2),

    % test for W = 3
    AccW3 = {length(Shards), length(Docs), list_to_integer("3"), GroupedDocs,
        Dict},

    {ok,{WaitingCountW3_1,_,_,_,_}=AccW3_1} =
        handle_message({ok, [{ok, Doc1}]},hd(Shards),AccW3),
    ?assertEqual(WaitingCountW3_1,2),

    {ok,{WaitingCountW3_2,_,_,_,_}=AccW3_2} =
        handle_message({ok, [{ok, Doc1}]},lists:nth(2,Shards),AccW3_1),
    ?assertEqual(WaitingCountW3_2,1),

    {stop, FinalReplyW3 } =
        handle_message({ok, [{ok, Doc1}]},lists:nth(3,Shards),AccW3_2),
    ?assertEqual([{Doc1, {ok,Doc1}}],FinalReplyW3),

    % test w quorum > # shards, which should fail immediately

    Shards2 = mem3_util:create_partition_map("foo",1,1,["node1"]),
    GroupedDocs2 = group_docs_by_shard_hack(<<"foo">>,Shards2,Docs),

    AccW4 =
        {length(Shards2), length(Docs), list_to_integer("2"), GroupedDocs2, Dict},
    Bool =
    case handle_message({ok, [{ok, Doc1}]},hd(Shards2),AccW4) of
        {stop, _Reply} ->
            true;
        _ -> false
    end,
    ?assertEqual(Bool,true),

    % two docs with exit messages
    GroupedDocs3 = group_docs_by_shard_hack(<<"foo">>,Shards,Docs2),
    AccW5 = {length(Shards), length(Docs2), list_to_integer("2"), GroupedDocs3,
        Dict2},

    {ok,{WaitingCountW5_1,_,_,_,_}=AccW5_1} =
        handle_message({ok, [{ok, Doc1}]},hd(Shards),AccW5),
    ?assertEqual(WaitingCountW5_1,2),

    {ok,{WaitingCountW5_2,_,_,_,_}=AccW5_2} =
        handle_message({rexi_EXIT, 1},lists:nth(2,Shards),AccW5_1),
    ?assertEqual(WaitingCountW5_2,1),

    {stop, ReplyW5} =
        handle_message({rexi_EXIT, 1},lists:nth(3,Shards),AccW5_2),

    ?assertEqual([{Doc1, noreply},{Doc2, {ok,Doc1}}],ReplyW5).


doc_update2_test() ->
    Doc1 = #doc{revs = {1,[<<"foo">>]}},
    Doc2 = #doc{revs = {1,[<<"bar">>]}},
    Docs = [Doc2, Doc1],
    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedDocs = group_docs_by_shard_hack(<<"foo">>,Shards,Docs),
    Acc0 = {length(Shards), length(Docs), list_to_integer("2"), GroupedDocs,
        dict:from_list([{Doc,[]} || Doc <- Docs])},

    {ok,{WaitingCount1,_,_,_,_}=Acc1} =
        handle_message({ok, [{ok, Doc1},{ok, Doc2}]},hd(Shards),Acc0),
    ?assertEqual(WaitingCount1,2),

    {ok,{WaitingCount2,_,_,_,_}=Acc2} =
        handle_message({rexi_EXIT, 1},lists:nth(2,Shards),Acc1),
    ?assertEqual(WaitingCount2,1),

    {stop, Reply} =
        handle_message({rexi_EXIT, 1},lists:nth(3,Shards),Acc2),

    ?assertEqual([{Doc1, {ok, Doc2}},{Doc2, {ok,Doc1}}],Reply).

doc_update3_test() ->
    Doc1 = #doc{revs = {1,[<<"foo">>]}},
    Doc2 = #doc{revs = {1,[<<"bar">>]}},
    Docs = [Doc2, Doc1],
    Shards =
        mem3_util:create_partition_map("foo",3,1,["node1","node2","node3"]),
    GroupedDocs = group_docs_by_shard_hack(<<"foo">>,Shards,Docs),
    Acc0 = {length(Shards), length(Docs), list_to_integer("2"), GroupedDocs,
        dict:from_list([{Doc,[]} || Doc <- Docs])},

    {ok,{WaitingCount1,_,_,_,_}=Acc1} =
        handle_message({ok, [{ok, Doc1},{ok, Doc2}]},hd(Shards),Acc0),
    ?assertEqual(WaitingCount1,2),

    {ok,{WaitingCount2,_,_,_,_}=Acc2} =
        handle_message({rexi_EXIT, 1},lists:nth(2,Shards),Acc1),
    ?assertEqual(WaitingCount2,1),

    {stop, Reply} =
        handle_message({ok, [{ok, Doc1},{ok, Doc2}]},lists:nth(3,Shards),Acc2),

    ?assertEqual([{Doc1, {ok, Doc2}},{Doc2, {ok,Doc1}}],Reply).

% needed for testing to avoid having to start the mem3 application
group_docs_by_shard_hack(_DbName, Shards, Docs) ->
    dict:to_list(lists:foldl(fun(#doc{id=_Id} = Doc, D0) ->
        Ref = make_ref(),
        lists:foldl(fun(Shard, D1) ->
            dict:append(Shard, {Doc, Ref}, D1)
        end, D0, Shards)
    end, dict:new(), Docs)).
