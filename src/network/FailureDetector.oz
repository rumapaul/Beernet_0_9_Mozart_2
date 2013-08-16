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
   Component   at '../corecomp/Component.ozf'
   PbeerList   at '../utils/PbeerList.ozf'
   Timer       at '../timer/Timer.ozf'
   System
export
   New
define

   DELTA       = 500    % Granularity to tune the failure detector
   TIMEOUT     = 500   % Initial Timeout value
   MAX_TIMEOUT = 2000   % Timeout must not go beyond this value
   
   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      SelfPbeer   % Pbeer reference assinged by a external component

      Alive       % Pbeers known to be alive
      Notified    % Pbeers already notified as crashed
      Pbeers      % Pbeers to be monitored
      Connections   % Pbeers register during a ping round
      TheTimer    % Component that triggers timeout

      %% Sends a ping message to all monitored pbeers and launch the timer
      proc {NewRound start(Pbeer)}
         {ComLayer sendTo(Pbeer ping(@SelfPbeer tag:fd) log:faildet)}
         {TheTimer startTrigger(Pbeer.period timeout(Pbeer))}
      end

      proc {Monitor monitor(Pbeer)}
         if {Not {PbeerList.isIn Pbeer @Pbeers}} then
            NewConnection
            in
            Pbeers := {PbeerList.add Pbeer @Pbeers}
            NewConnection = {Record.adjoinAt Pbeer period TIMEOUT}
            Connections := {PbeerList.add NewConnection @Connections}
            {NewRound start(NewConnection)}
         end
      end

      proc {Timeout timeout(ConnectionPbeer)}
         Pbeer
         in
         Connections := {PbeerList.remove ConnectionPbeer @Connections}
         Pbeer = {PbeerList.retrievePbeer ConnectionPbeer.id @Pbeers}
         if Pbeer \= nil then
           Period = {NewCell ConnectionPbeer.period}
           NewConnection
           in
           if {PbeerList.isIn ConnectionPbeer @Alive} andthen
               {PbeerList.isIn ConnectionPbeer @Notified} then
                  Notified := {PbeerList.remove ConnectionPbeer @Notified}
                  {@Listener alive(Pbeer)}
                      
	          if @Period < MAX_TIMEOUT then
                     Period := @Period + DELTA
                  end
           end

           NewConnection = {Record.adjoinAt Pbeer period @Period}

           if {Not {PbeerList.isIn ConnectionPbeer @Alive}} andthen
              {Not {PbeerList.isIn ConnectionPbeer @Notified}} then
                Notified := {PbeerList.add NewConnection @Notified}
                {@Listener crash(Pbeer)}
           end
           %% Clear up and get ready for new ping round
           Alive       := {PbeerList.remove ConnectionPbeer @Alive}
           Connections := {PbeerList.add NewConnection @Connections}

           {NewRound start(NewConnection)}
        end
      end

      proc {Ping ping(Pbeer tag:fd)}
         {ComLayer sendTo(Pbeer pong(@SelfPbeer tag:fd) log:faildet)}
      end

      proc {Pong pong(Pbeer tag:fd)}
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
      %Period      = {NewCell TIMEOUT}
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      Self        = {Component.new Events}
      Listener    = Self.listener
      {TheTimer setListener(Self.trigger)}
      
      %{NewRound start}
      Self.trigger 
   end
end

