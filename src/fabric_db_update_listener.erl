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

-module(fabric_db_update_listener).

-export([go/3, start_update_notifier/1]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").

go(Parent, DbName, Timeout) ->
    spawn_link(fun() ->
                   Notifiers = start_update_notifiers(DbName),
                   MonRefs = lists:usort([{rexi_server, Node} || {Node, _Ref} <- Notifiers]),
                   RexiMon = rexi_monitor:start(MonRefs),
                   %% Add calling controller node as it needs to send messages also
                   Workers = [{node(), make_ref()} | Notifiers],
                   try
                       rexi_utils:recv(Workers, 2, fun handle_message/3, {Workers, Parent, unset},
                                       infinity, Timeout)
                   after
                       rexi_monitor:stop(RexiMon),
                       stop_update_notifiers(Notifiers)
                   end
               end).

start_update_notifiers(DbName) ->
    lists:map(fun(#shard{node=Node, name=Name}) ->
        {Node, rexi:cast(Node, {?MODULE, start_update_notifier, [Name]})}
    end, mem3:shards(DbName)).

% rexi endpoint
start_update_notifier(DbName) ->
    Fun = fun({_, X}) when X == DbName -> rexi:reply(db_updated); (_) -> ok end,
    Id = {couch_db_update_notifier, make_ref()},
    ok = gen_event:add_sup_handler(couch_db_update, Id, Fun),
    receive {gen_event_EXIT, Id, Reason} ->
        rexi:reply({gen_event_EXIT, DbName, Reason})
    end.

stop_update_notifiers(Notifiers) ->
    [rexi:kill(Node, Ref) || {Node, Ref} <- Notifiers].

handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Worker, {Workers, Parent, State}) ->
    NewWorkers = lists:filter(fun({_Node, Ref}) -> NodeRef =/= Ref end, Workers),
    case NewWorkers of
    [] ->
        {error, {nodedown, <<"progress not possible">>}};
    _ ->
        {ok, {NewWorkers, Parent, State}}
    end;
handle_message({rexi_EXIT, Reason}, Worker, {Workers, Parent, State}) ->
    NewWorkers = lists:delete(Worker,Workers),
    case NewWorkers of
    [] ->
        {error, Reason};
    _ ->
        {ok, {NewWorkers, Parent, State}}
    end;
handle_message(db_updated, _Worker, {Workers, Parent, waiting}) ->
    Parent ! updated,
    {ok, {Workers, Parent, unset}};
handle_message(db_updated, _Worker, {Workers, Parent, State})
  when State == unset orelse State == updated ->
    {ok, {Workers, Parent, updated}};
handle_message(get_state, _Worker, {Workers, Parent, unset}) ->
    {ok, {Workers, Parent, waiting}};
handle_message(get_state, _Worker, {Workers, Parent, State}) ->
    Parent ! State,
    {ok, {Workers, Parent, unset}};
handle_message(timeout, _Worker, {Workers, Parent, State}) ->
    case State of
    waiting ->
        Parent ! timeout,
        {ok, {Workers, Parent, unset}};
    unset ->
        {ok, {Workers, Parent, State}}
    end.



