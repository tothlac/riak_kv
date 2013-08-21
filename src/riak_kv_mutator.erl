%% -------------------------------------------------------------------
%%
%% riak_kv_mutators - Storage and retrieval for get/put mutation
%% functions
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc There are circumstances where the object stored on disk is not
%% the object to return; and there are times the object written to the
%% data storage backend is not meant to be the object given. An
%% example would be storing only meta data for an object on a remote
%% cluster. This module is an interface to register mutators that will
%% can be run.
%%
%% This doubles as a behavior defining module for the mutators.

-module(riak_kv_mutator).

-export([register/1, unregister/1]).
-export([get/0]).
-export([mutate_put/2, mutate_get/2]).

register(Module) ->
    Modifier = fun
        (undefined) ->
            [Module];
        (Values) ->
            Values2 = lists:filter(fun erlang:is_list/1, Values),
            Values3 = ordsets:union(Values2),
            ordsets:add_element(Module, Values3)
    end,
    riak_core_metadata:put({kv, mutators}, list, Modifier).

unregister(Module) ->
    Modifier = fun
        (undefined) ->
            [];
        (Values) ->
            Values2 = lists:filter(fun erlang:is_list/1, Values),
            Values3 = ordsets:union(Values2),
            ordsets:del_element(Module, Values3)
    end,
    riak_core_metadata:put({kv, mutators}, list, Modifier, []).

get() ->
    Resolver = fun(Values) ->
        Values2 = lists:filter(fun erlang:is_list/1, Values),
        ordsets:union(Values2)
    end,
    Modules = riak_core_metadata:get({kv, mutators}, list, [{default, []}, {resolver, Resolver}]),
    {ok, Modules}.

mutate_get(Object, BucketProps) ->
    mutate(Object, BucketProps, mutate_get).

mutate_put(Object, BucketProps) ->
    mutate(Object, BucketProps, mutate_put).

mutate(Object, BucketProps, Func) ->
    FoldFun = fun(Module, Obj) ->
        Module:Func(Obj, BucketProps)
    end,
    {ok, Modules} = ?MODULE:get(),
    lists:foldl(FoldFun, Object, Modules).