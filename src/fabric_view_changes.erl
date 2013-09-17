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

-module(fabric_view_changes).

-export([go/5, pack_seqs/1, unpack_seqs/2]).

%% exported for upgrade purposes.
-export([keep_sending_changes/8]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(fabric_db_update_listener, [wait_db_updated/1]).

go(DbName, Feed, Options, Callback, Acc0) when Feed == "continuous" orelse
        Feed == "longpoll" ->
    Args = make_changes_args(Options),
    Since = get_start_seq(DbName, Args),
    case validate_start_seq(DbName, Since) of
    ok ->
        {ok, Acc} = Callback(start, Acc0),
        {Timeout, _} = couch_changes:get_changes_timeout(Args, Callback),
        Ref = make_ref(),
        Parent = self(),
        UpdateListener = {spawn_link(fabric_db_update_listener, go,
                                     [Parent, Ref, DbName, Timeout]),
                          Ref},
        try
            keep_sending_changes(
                DbName,
                Args,
                Callback,
                Since,
                Acc,
                Timeout,
                UpdateListener,
                os:timestamp()
            )
        after
            fabric_db_update_listener:stop(UpdateListener)
        end;
    Error ->
        Callback(Error, Acc0)
    end;

go(DbName, "normal", Options, Callback, Acc0) ->
    Args = make_changes_args(Options),
    Since = get_start_seq(DbName, Args),
    case validate_start_seq(DbName, Since) of
    ok ->
        {ok, Acc} = Callback(start, Acc0),
        {ok, #collector{counters=Seqs, user_acc=AccOut}} = send_changes(
            DbName,
            Args,
            Callback,
            Since,
            Acc,
            5000
        ),
        Callback({stop, pack_seqs(Seqs)}, AccOut);
    Error ->
        Callback(Error, Acc0)
    end.

keep_sending_changes(DbName, Args, Callback, Seqs, AccIn, Timeout, UpListen, T0) ->
    #changes_args{limit=Limit, feed=Feed, heartbeat=Heartbeat} = Args,
    {ok, Collector} = send_changes(DbName, Args, Callback, Seqs, AccIn, Timeout),
    #collector{limit=Limit2, counters=NewSeqs, user_acc=AccOut} = Collector,
    LastSeq = pack_seqs(NewSeqs),
    MaintenanceMode = config:get("cloudant", "maintenance_mode"),
    if Limit > Limit2, Feed == "longpoll";
      MaintenanceMode == "true"; MaintenanceMode == "nolb" ->
        Callback({stop, LastSeq}, AccOut);
    true ->
        WaitForUpdate = wait_db_updated(UpListen),
        AccumulatedTime = timer:now_diff(os:timestamp(), T0) div 1000,
        Max = case config:get("fabric", "changes_duration") of
        undefined ->
            infinity;
        MaxStr ->
            list_to_integer(MaxStr)
        end,
        case {Heartbeat, AccumulatedTime > Max, WaitForUpdate} of
        {undefined, _, timeout} ->
            Callback({stop, LastSeq}, AccOut);
        {_, true, timeout} ->
            Callback({stop, LastSeq}, AccOut);
        _ ->
            {ok, AccTimeout} = Callback(timeout, AccOut),
            ?MODULE:keep_sending_changes(
                DbName,
                Args#changes_args{limit=Limit2},
                Callback,
                LastSeq,
                AccTimeout,
                Timeout,
                UpListen,
                T0
            )
        end
    end.

send_changes(DbName, ChangesArgs, Callback, PackedSeqs, AccIn, Timeout) ->
    LiveNodes = [node() | nodes()],
    AllLiveShards = mem3:live_shards(DbName, LiveNodes),
    Seqs = lists:flatmap(fun({#shard{name=Name, node=N} = Shard, Seq}) ->
        case lists:member(Shard, AllLiveShards) of
        true ->
            Ref = rexi:cast(N, {fabric_rpc, changes, [Name,ChangesArgs,Seq]}),
            [{Shard#shard{ref = Ref}, Seq}];
        false ->
            % Find some replacement shards to cover the missing range
            % TODO It's possible in rare cases of shard merging to end up
            % with overlapping shard ranges from this technique
            lists:map(fun(#shard{name=Name2, node=N2} = NewShard) ->
                Ref = rexi:cast(N2, {fabric_rpc, changes, [Name2,ChangesArgs,0]}),
                {NewShard#shard{ref = Ref}, 0}
            end, find_replacement_shards(Shard, AllLiveShards))
        end
    end, unpack_seqs(PackedSeqs, DbName)),
    {Workers, _} = lists:unzip(Seqs),
    RexiMon = fabric_util:create_monitors(Workers),
    State = #collector{
        query_args = ChangesArgs,
        callback = Callback,
        counters = orddict:from_list(Seqs),
        user_acc = AccIn,
        limit = ChangesArgs#changes_args.limit,
        rows = Seqs % store sequence positions instead
    },
    %% TODO: errors need to be handled here
    try
        receive_results(Workers, State, Timeout, Callback)
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.

receive_results(Workers, State, Timeout, Callback) ->
    case rexi_utils:recv(Workers, #shard.ref, fun handle_message/3, State,
            Timeout, infinity) of
    {timeout, NewState0} ->
        {ok, AccOut} = Callback(timeout, NewState0#collector.user_acc),
        NewState = NewState0#collector{user_acc = AccOut},
        receive_results(Workers, NewState, Timeout, Callback);
    {_, NewState} ->
        {ok, NewState}
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, nil, State) ->
    fabric_view:remove_down_shards(State, NodeRef);

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    #collector{
        callback=Callback,
        counters=Counters0,
        rows = Seqs0,
        user_acc=Acc
    } = State,
    Counters = fabric_dict:erase(Worker, Counters0),
    Seqs = fabric_dict:erase(Worker, Seqs0),
    case fabric_view:is_progress_possible(Counters) of
    true ->
        {ok, State#collector{counters = Counters, rows=Seqs}};
    false ->
        {ok, Resp} = Callback({error, fabric_util:error_info(Reason)}, Acc),
        {error, Resp}
    end;

handle_message(_, _, #collector{limit=0} = State) ->
    {stop, State};

handle_message(#change{key=Key} = Row0, {Worker, From}, St) ->
    #collector{
        query_args = #changes_args{include_docs=IncludeDocs},
        callback = Callback,
        counters = S0,
        limit = Limit,
        user_acc = AccIn
    } = St,
    case fabric_dict:lookup_element(Worker, S0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate it
        gen_server:reply(From, stop),
        {ok, St};
    _ ->
        S1 = fabric_dict:store(Worker, Key, S0),
        S2 = fabric_view:remove_overlapping_shards(Worker, S1),
        % this check should not be necessary at all, as holes in the ranges
        % created from DOWN messages would have led to errors
        case fabric_view:is_progress_possible(S2) of
        true ->
            Row = Row0#change{key = pack_seqs(S2)},
            {Go, Acc} = Callback(changes_row(Row, IncludeDocs), AccIn),
            gen_server:reply(From, Go),
            {Go, St#collector{counters=S2, limit=Limit-1, user_acc=Acc}};
        false ->
            Reason = {range_not_covered, <<"progress not possible">>},
            Callback({error, Reason}, AccIn),
            gen_server:reply(From, stop),
            {stop, St#collector{counters=S2}}
        end
    end;

handle_message({no_pass, Seq}, {Worker, From}, St) ->
    #collector{
        counters = S0
    } = St,
    case fabric_dict:lookup_element(Worker, S0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate it
        gen_server:reply(From, stop),
        {ok, St};
    _ ->
        S1 = fabric_dict:store(Worker, Seq, S0),
        S2 = fabric_view:remove_overlapping_shards(Worker, S1),
        gen_server:reply(From, ok),
        {ok, St#collector{counters=S2}}
    end;

handle_message({complete, Key}, Worker, State) ->
    #collector{
        callback = Callback,
        counters = S0,
        total_rows = Completed, % override
        user_acc = Acc
    } = State,
    case fabric_dict:lookup_element(Worker, S0) of
    undefined ->
        {ok, State};
    _ ->
        S1 = fabric_dict:store(Worker, Key, S0),
        % unlikely to have overlaps here, but possible w/ filters
        S2 = fabric_view:remove_overlapping_shards(Worker, S1),
        NewState = State#collector{counters=S2, total_rows=Completed+1},
        case fabric_dict:size(S2) =:= (Completed+1) of
        true ->
            % check ranges are covered, again this should not be neccessary
            % as any holes in the ranges due to DOWN messages would have errored
            % out sooner
            case fabric_view:is_progress_possible(S2) of
            true ->
                {stop, NewState};
            false ->
                Reason = {range_not_covered, <<"progress not possible">>},
                Callback({error, Reason}, Acc),
                {stop, NewState}
            end;
        false ->
            {ok, NewState}
        end
    end.

make_changes_args(#changes_args{style=Style, filter=undefined}=Args) ->
    Args#changes_args{filter = Style};
make_changes_args(Args) ->
    Args.

get_start_seq(_DbName, #changes_args{dir=fwd, since=Since}) ->
    Since;
get_start_seq(DbName, #changes_args{dir=rev}) ->
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, get_update_seq, []),
    {ok, Since} = fabric_util:recv(Workers, #shard.ref,
        fun collect_update_seqs/3, fabric_dict:init(Workers, -1)),
    Since.

collect_update_seqs(Seq, Shard, Counters) when is_integer(Seq) ->
    case fabric_dict:lookup_element(Shard, Counters) of
    undefined ->
        % already heard from someone else in this range
        {ok, Counters};
    -1 ->
        C1 = fabric_dict:store(Shard, Seq, Counters),
        C2 = fabric_view:remove_overlapping_shards(Shard, C1),
        case fabric_dict:any(-1, C2) of
        true ->
            {ok, C2};
        false ->
            {stop, pack_seqs(C2)}
        end
    end.

pack_seqs(Workers) ->
    SeqList = [{N,R,S} || {#shard{node=N, range=R}, S} <- Workers],
    SeqSum = lists:sum([seq(S) || {_,_,S} <- SeqList]),
    Opaque = couch_util:encodeBase64Url(term_to_binary(SeqList, [compressed])),
    [SeqSum, Opaque].

seq({Seq, _Uuid, _Node}) -> Seq; % remove me
seq({Seq, _Uuid}) -> Seq;
seq(Seq)          -> Seq.

unpack_seqs(0, DbName) ->
    fabric_dict:init(mem3:shards(DbName), 0);

unpack_seqs("0", DbName) ->
    fabric_dict:init(mem3:shards(DbName), 0);

unpack_seqs([_SeqNum, Opaque], DbName) ->
    do_unpack_seqs(Opaque, DbName);

unpack_seqs(Packed, DbName) ->
    NewPattern = "^\\[[0-9]+\s*,\s*\"(?<opaque>.*)\"\\]$",
    OldPattern = "^\"?([0-9]+-)?(?<opaque>.*?)\"?$",
    Options = [{capture, [opaque], binary}],
    Opaque = case re:run(Packed, NewPattern, Options) of
    {match, Match} ->
        Match;
    nomatch ->
        {match, Match} = re:run(Packed, OldPattern, Options),
        Match
    end,
    do_unpack_seqs(Opaque, DbName).

do_unpack_seqs(Opaque, DbName) ->
    % A preventative fix for FB 13533 to remove duplicate shards.
    % This just picks each unique shard and keeps the largest seq
    % value recorded.
    Decoded = binary_to_term(couch_util:decodeBase64Url(Opaque)),
    DedupDict = lists:foldl(fun({Node, [A, B], Seq}, Acc) ->
        dict:append({Node, [A, B]}, Seq, Acc)
    end, dict:new(), Decoded),
    Deduped = lists:map(fun({{Node, [A, B]}, SeqList}) ->
        {Node, [A, B], lists:max(SeqList)}
    end, dict:to_list(DedupDict)),

    % Create a fabric_dict of {Shard, Seq} entries
    % TODO relies on internal structure of fabric_dict as keylist
    Unpacked = lists:flatmap(fun({Node, [A,B], Seq}) ->
        case mem3:get_shard(DbName, Node, [A,B]) of
        {ok, Shard} ->
            [{Shard, Seq}];
        {error, not_found} ->
            []
        end
    end, Deduped),

    % Fill holes in the since sequence. If/when we ever start
    % using overlapping shard ranges this will need to be updated
    % to not include shard ranges that overlap entries in Upacked.
    % A quick and dirty approach would be like such:
    %
    %   lists:foldl(fun(S, Acc) ->
    %       fabric_view:remove_overlapping_shards(S, Acc)
    %   end, mem3:shards(DbName), Unpacked)
    %
    % Unfortunately remove_overlapping_shards isn't reusable because
    % of its calls to rexi:kill/2. When we get to overlapping
    % shard ranges and have to rewrite shard range management
    % we can revisit this simpler algorithm.
    case fabric_view:is_progress_possible(Unpacked) of
        true ->
            Unpacked;
        false ->
            Extract = fun({Shard, _Seq}) -> Shard#shard.range end,
            Ranges = lists:usort(lists:map(Extract, Unpacked)),
            Filter = fun(S) -> not lists:member(S#shard.range, Ranges) end,
            Replacements = lists:filter(Filter, mem3:shards(DbName)),
            Unpacked ++ [{R, 0} || R <- Replacements]
    end.

changes_row(#change{key=Seq, id=Id, value=Value, deleted=true, doc=Doc}, true) ->
    {change, {[{seq,Seq}, {id,Id}, {changes,Value}, {deleted, true}, {doc, Doc}]}};
changes_row(#change{key=Seq, id=Id, value=Value, deleted=true}, false) ->
    {change, {[{seq,Seq}, {id,Id}, {changes,Value}, {deleted, true}]}};
changes_row(#change{key=Seq, id=Id, value=Value, doc={error,Reason}}, true) ->
    {change, {[{seq,Seq}, {id,Id}, {changes,Value}, {error,Reason}]}};
changes_row(#change{key=Seq, id=Id, value=Value, doc=Doc}, true) ->
    {change, {[{seq,Seq}, {id,Id}, {changes,Value}, {doc,Doc}]}};
changes_row(#change{key=Seq, id=Id, value=Value}, false) ->
    {change, {[{seq,Seq}, {id,Id}, {changes,Value}]}}.

find_replacement_shards(#shard{range=Range}, AllShards) ->
    % TODO make this moar betta -- we might have split or merged the partition
    [Shard || Shard <- AllShards, Shard#shard.range =:= Range].

validate_start_seq(DbName, Seq) ->
    try unpack_seqs(Seq, DbName) of _Any ->
        ok
    catch
        error:database_does_not_exist ->
            {error, database_does_not_exist};
        _:_ ->
            Reason = <<"Malformed sequence supplied in 'since' parameter.">>,
            {error, {bad_request, Reason}}
    end.

unpack_seqs_test() ->
    meck:new(mem3),
    meck:new(fabric_view),
    meck:expect(mem3, get_shard, fun(_, _, _) -> {ok, #shard{}} end),
    meck:expect(fabric_view, is_progress_possible, fun(_) -> true end),

    % BigCouch 0.3 style.
    assert_shards("23423-g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA"),

    % BigCouch 0.4 style.
    assert_shards([23423,<<"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA">>]),

    % BigCouch 0.4 style (as string).
    assert_shards("[23423,\"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA\"]"),
    assert_shards("[23423 ,\"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA\"]"),
    assert_shards("[23423, \"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA\"]"),
    assert_shards("[23423 , \"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA\"]"),

    % with internal hypen
    assert_shards("651-g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwNDLXMwBCwxygOFMiQ"
    "5L8____sxJTcalIUgCSSfZgReE4FTmAFMWDFYXgVJQAUlQPVuSKS1EeC5BkaABSQHXz8"
    "VgJUbgAonB_VqIPfoUHIArvE7T6AUQh0I1-WQAzp1XB"),
    assert_shards([651,"g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwNDLXMwBCwxygOFMiQ"
    "5L8____sxJTcalIUgCSSfZgReE4FTmAFMWDFYXgVJQAUlQPVuSKS1EeC5BkaABSQHXz8"
    "VgJUbgAonB_VqIPfoUHIArvE7T6AUQh0I1-WQAzp1XB"]),

    % CouchDB 1.2 style
    assert_shards("\"23423-g1AAAAE7eJzLYWBg4MhgTmHgS0ktM3QwND"
    "LXMwBCwxygOFMiQ5L8____sxIZcKlIUgCSSfZgRUw4FTmAFMWDFTHiVJQAUlSPX1Ee"
    "C5BkaABSQHXzsxKZ8StcAFG4H4_bIAoPQBTeJ2j1A4hCUJBkAQC7U1NA\""),

    meck:unload(fabric_view),
    meck:unload(mem3).

assert_shards(Packed) ->
    ?assertMatch([{#shard{},_}|_], unpack_seqs(Packed, <<"foo">>)).
