;
; #!
; %
; Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
; %
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
; 
;      http://www.apache.org/licenses/LICENSE-2.0
; 
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
; #_
;
(ns de.hub.cs.dbis.aeolus.batching.StormConnector
(:require [backtype.storm.daemon [executor :as executor]])
(:gen-class
  :methods [#^{:static true} [getFieldsGroupingReceiverTaskId [backtype.storm.task.WorkerTopologyContext String String String java.util.List] Integer]]))

(defn -getFieldsGroupingReceiverTaskId
  "Returns the receiver task ID for "
  [context producer-component-id stream-id receiver-component-id tuple]
  (let [stream-receiver-groupingFn-map (executor/outbound-components context producer-component-id)]
    ((get (get stream-receiver-groupingFn-map stream-id) receiver-component-id) producer-component-id tuple))
)
