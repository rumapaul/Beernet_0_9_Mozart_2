/*-------------------------------------------------------------------------
 *
 * FailureDetector.oz
 *
 *    Eventually perfect failure detector
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: boriss $
 *
 *    $Date: 2011-05-19 21:45:21 +0200 (Thu, 19 May 2011) $
 *
 * NOTES
 *      
 *    Sends keep alive messages to other nodes, and triggers crash event upon
 *    timeout without answer. Event alive is trigger to fix a false suspicion.
 *
 * EVENTS
 *
 *    Accepts: eventName(arg1 argN) - Events that can be triggered on this
 *    component to request a service.
 *
 *    Confirmation: eventName(arg1 argN) - Events used by a component to
 *    confirm the completion of a request.
 *
 *    Indication: eventName(arg1 argN) - Events used by a given component to
 *    deliver information to another component
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   BootTime at 'x-oz://boot/Time'
   Component   at '../corecomp/Component.ozf'
   PbeerList   at '../utils/PbeerList.ozf'
   Timer       at '../timer/Timer.ozf'
   %System
   
export
   New
define

   INIT_TIMEOUT = 500   % Initial Timeout value
   MIN_TIMEOUT = 100     % Minimum Timeout value
   BUFFER_LIMIT = 20 	% Recent History of Round Trip Time
   MONITORING_LIMIT = 75 % Maximum nodes that will be monitored by a node for a network size of 50

   %% Add an element at the end of list.
   %% Return the new list as result
   fun {ModifyABoundedList Element L}
      fun {Insert E List}
         case List
            of H|T then
               H|{Insert E T}
         [] nil then
              Element|nil
         end
      end
      fun {Delete List}
         case List
           of _|T then
              T
         [] nil then
              nil
         end
      end
      NewList
      in
     
      if {List.length L} >= BUFFER_LIMIT then
          NewList = {Delete L}
      else
          NewList = L
      end
      {Insert Element NewList}
   end

   fun {CalculateTimeout L}
      fun {CalculateVarianceStep List Avg}
         case List
            of H|T then
            {Number.pow {Number.abs (Avg-H)} 2} + {CalculateVarianceStep T Avg}
         [] nil then
           0
         end
      end
      TotalRTT
      AvgRTT
      CurrentVariance
      CurrentStDev
      CurrentCount = {List.length L}
      RetVal
      in
      TotalRTT = {List.foldL L fun {$ X Y} X+Y end 0}
      AvgRTT = TotalRTT div CurrentCount
      CurrentVariance = {CalculateVarianceStep L AvgRTT} div CurrentCount
      CurrentStDev = {Float.toInt {Float.ceil {Float.sqrt {Int.toFloat CurrentVariance}}}}

      %RetVal = {Value.max (AvgRTT + CurrentVariance) MIN_TIMEOUT}
      RetVal = {Value.max (AvgRTT + CurrentStDev) MIN_TIMEOUT}
      
      RetVal
   end

   fun {FindOldestSuspicion SuspicionList ConnectionList}
      proc {SuspectedRound L ObservedOldVal ResultPbeer}
          case L
           of H|T then
              CurrentConnection = {PbeerList.retrievePbeer H.id ConnectionList}
              in
              if CurrentConnection.last_response < ObservedOldVal then
                 ObservedOldVal := CurrentConnection.last_response
                 ResultPbeer := H
              end
              {SuspectedRound T ObservedOldVal ResultPbeer}
           [] nil then
              skip
           end
      end
      ObservedOldest
      RPbeer
      in
      ObservedOldest = {NewCell {BootTime.getReferenceTime}}
      RPbeer = {NewCell nil}
      {SuspectedRound SuspicionList ObservedOldest RPbeer}
      @RPbeer
   end

   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      SelfPbeer   % Pbeer reference assinged by a external component

      Alive       % Pbeers known to be alive
      Notified    % Pbeers already notified as crashed
      Pbeers      % Pbeers to be monitored
      Connections   % Connection parameters for all the monitored Pbeers
      TheTimer    % Component that triggers timeout

      %% Sends a ping message to all monitored pbeers and launch the timer
      proc {NewRound start(Pbeer T)}
         {ComLayer sendTo(Pbeer ping(@SelfPbeer
			timestamp:{BootTime.getReferenceTime} tag:fd) log:faildet)}
         {TheTimer startTrigger(T timeout(Pbeer.id))}
      end

      proc {Monitor monitor(Pbeer)}
         if Pbeer.id \= @SelfPbeer.id andthen
            {Not {PbeerList.isIn Pbeer @Pbeers}} then
            NewConnection
            in
            Pbeers := {PbeerList.add Pbeer @Pbeers}
            NewConnection = {Record.adjoinAt Pbeer rtt_history nil}
            Connections := {PbeerList.add {Record.adjoinAt NewConnection last_response 0} @Connections}
            {NewRound start(Pbeer INIT_TIMEOUT)}
         end

         if {List.length @Pbeers} > MONITORING_LIMIT andthen {List.length @Notified} > 0 then
             ToBeDeletedPbeer = {FindOldestSuspicion @Notified @Connections} 
             in
             if ToBeDeletedPbeer\= nil then
                 Pbeers := {PbeerList.remove ToBeDeletedPbeer @Pbeers}
                 Notified := {PbeerList.remove ToBeDeletedPbeer @Notified}
             end
         end
      end

      proc {Timeout timeout(PbeerId)}
         Pbeer
         CurrentConnection
         in
         CurrentConnection = {PbeerList.retrievePbeer PbeerId @Connections} 
         Pbeer = {PbeerList.retrievePbeer PbeerId @Pbeers}
         
         if Pbeer \= nil then
           IsInAlive = {PbeerList.isIn Pbeer @Alive}
           IsInNotified = {PbeerList.isIn Pbeer @Notified}
           NewTimeout
           in 
           if IsInAlive andthen IsInNotified then
                  Notified := {PbeerList.remove Pbeer @Notified}
                  {@Listener alive(Pbeer)}
           end  

           if {Not IsInAlive} andthen {Not IsInNotified} then
                Notified := {PbeerList.add Pbeer @Notified}
                {@Listener crash(Pbeer)}
           end
           %% Clear up and get ready for new ping round
           Alive       := {PbeerList.remove Pbeer @Alive}
           if {List.length CurrentConnection.rtt_history} > 0 then
           %if {List.length CurrentConnection.rtt_history} >= BUFFER_LIMIT then
           	NewTimeout = {CalculateTimeout CurrentConnection.rtt_history}
           else
                NewTimeout = INIT_TIMEOUT
           end
           {NewRound start(Pbeer NewTimeout)}
        else
           Connections := {PbeerList.remove CurrentConnection @Connections}   
        end
      end

      proc {Ping ping(Pbeer timestamp:SentTime tag:fd)}
         {ComLayer sendTo(Pbeer pong(@SelfPbeer timestamp:SentTime tag:fd) log:faildet)}
      end

      proc {Pong pong(Pbeer timestamp:SentTime tag:fd)}
         CurrentConnection
         RTList
         CurrentRTT
         CurrentRefTime
         in
         CurrentRefTime = {BootTime.getReferenceTime} 
         CurrentRTT = CurrentRefTime-SentTime
         CurrentConnection = {Record.adjoinAt {PbeerList.retrievePbeer Pbeer.id @Connections} 								last_response CurrentRefTime}
         RTList = {ModifyABoundedList CurrentRTT CurrentConnection.rtt_history}
         Connections := {PbeerList.edit {Record.adjoinAt CurrentConnection 
                                          rtt_history RTList} @Connections}
         Alive := {PbeerList.add Pbeer @Alive}
      end

      proc {SetPbeer setPbeer(NewPbeer)}
         SelfPbeer := NewPbeer
      end

      proc {SetComLayer setComLayer(TheComLayer)}
         ComLayer = TheComLayer
         SelfPbeer := {ComLayer getRef($)} 
      end

      proc {StopMonitor stopMonitor(Pbeer)}
         Pbeers := {PbeerList.remove Pbeer @Pbeers}
      end

      Events = events(
                  monitor:       Monitor
                  ping:          Ping
                  pong:          Pong
                  setPbeer:      SetPbeer
                  setComLayer:   SetComLayer
                  stopMonitor:   StopMonitor
                  start:         NewRound
                  timeout:       Timeout
                  )
   in
      Pbeers      = {NewCell {PbeerList.new}}
      Connections   = {NewCell {PbeerList.new}}
      Alive       = {NewCell {PbeerList.new}} 
      Notified    = {NewCell {PbeerList.new}}

      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      Self        = {Component.new Events}
      Listener    = Self.listener
      {TheTimer setListener(Self.trigger)}
      
      Self.trigger 
   end
end

