-module(fabric_clock).

-export([
    new/0,

    from_string/1,
    to_string/1,

    add/3,
    add/4
]).


-include_lib("mem3/include/mem3.hrl").


-define(CURRENT_VERSION, 1).


-record(fabric_clock, {
    members
}).


new() ->
    #fabric_clock{}.


from_string(Opaque) when is_binary(Opaque) ->
    Decoded = binary_to_term(couch_util:decodeBase64Url(Opaque), [safe]),
    from_string(Decoded, #fabric_clock{});
from_string(Opaque) ->
    from_string(iolist_to_binary(Opaque)).


to_string(#fabric_clock{members=Members}) ->
    ByNodeDict = lists:foldl(fun({Node, Range, Seq}, Acc) ->
        dict:append(Node, {Range, Seq}, Acc)
    end, dict:new(), Members),
    ByNode0 = dict:to_list(ByNodeDict),
    ByNode1 = [{atom_to_list(Node), Vals} || {Node, Vals} <- ByNode0],
    couch_util:encodeBase64Url(term_to_binary({?CURRENT_VERSION, ByNode1})).


add(Clock, #shard{node=Node, range=Range}, UpdateSeq) ->
    add(Clock, Node, Range, UpdateSeq).


add(#fabric_clock{members=Members}=Clock, Node, Range, UpdateSeq) ->
    Clock#fabric_clock{
        members = dedupe([{Node, Range, UpdateSeq} | Members])
    }.


% Private API


from_string({Version, Data}, Clock) ->
    from_string(Version, Data, Clock).


from_string(1, Members, Clock) ->
    Clock#fabric_clock{
        members = dedupe(v1_members(Members, []))
    }.


v1_members([], Acc) ->
    Acc;
v1_members([{NodeList, Vals} | Rest], Acc) when is_list(NodeList) ->
    Node = list_to_existing_atom(NodeList),
    ValidateFun = fun({[Beg, End], Seq}) ->
        true = is_number(Beg),
        true = is_number(End),
        true = is_number(Seq) andalso Seq >= 0
    end,
    lists:foreach(ValidateFun, Vals),
    NewAcc = [{Node, Range, Seq} || {Range, Seq} <- Vals] ++ Acc,
    v1_members(Rest, NewAcc).


dedupe(Members) ->
    lists:foldl(fun
        (Member, []) ->
            [Member];
        ({N, R, S1}, [{N, R, S2} | Rest]) ->
            [{N, R, erlang:max(S1, S2)} | Rest];
        (Member, Acc) ->
            [Member | Acc]
    end, lists:usort(Members)).
