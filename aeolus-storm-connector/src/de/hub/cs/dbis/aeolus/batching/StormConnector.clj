(ns de.hub.cs.dbis.aeolus.batching.StormConnector
  (:require [backtype.storm.daemon [executor :as executor]])
  (:gen-class
    :methods [#^{:static true} [getFieldsGroupingReceiverTaskId [backtype.storm.task.WorkerTopologyContext, String, String, String, java.util.List] Integer]]))

(defn -getFieldsGroupingReceiverTaskId
  "Returns the receiver task ID for "
  [context producer-component-id stream-id receiver-component-id tuple]
  (let [stream-receiver-groupingFn-map (executor/outbound-components context producer-component-id)]
    ((get (get stream-receiver-groupingFn-map stream-id) receiver-component-id) producer-component-id tuple))
)
