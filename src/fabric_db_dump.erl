% Copyright 2013 Cloudant
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

-module(fabric_db_dump).

-include("fabric.hrl").


-export([go/4]).


go(DbName, Callback, Acc, Args) ->
    Wrapper = fun(Event, WrapperAcc) ->
        changes_callback(Event, WrapperAcc, Callback)
    end,
    fabric:changes(DbName, Wrapper, Acc, [{dump, true}|Args]).


changes_callback(start, Acc, Callback) ->
    Callback(start, Acc);

changes_callback({change, {Change}}, Acc, Callback) ->
    DumpChange = #dump_change{
        type=couch_util:get_value(type, Change),
        ref=couch_util:get_value(ref, Change),
        change=couch_util:get_value(change, Change),
        seq=couch_util:get_value(seq, Change)
    },
    Callback(DumpChange, Acc);

changes_callback({stop, EndSeq}, Acc, Callback) ->
    Callback({stop, EndSeq}, Acc);

changes_callback({error, Error}, Acc, Callback) ->
    Callback({error, Error}, Acc).
