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
      NewPbeers   % Pbeers register during a ping round
      Period      % Period of time to time out
      TheTimer    % Component that triggers timeout

      %DebugVar

      %% Sends a ping message to all monitored pbeers and launch the timer
      proc {NewRound start}
         for Pbeer in @Pbeers do
            {ComLayer sendTo(Pbeer ping(@SelfPbeer tag:fd) log:faildet)}
         end
         {TheTimer startTimer(@Period)}
      end

      proc {Monitor monitor(Pbeer)}
         NewPbeers := {PbeerList.add Pbeer @NewPbeers}
      end

      /*proc {PrintAList L}
        for Pbeer in L do
          {System.showInfo Pbeer.id}
        end
        {System.showInfo "\n"}
      end*/

      proc {Timeout timeout}
         Resurrected Suspected
         in
         Resurrected = {PbeerList.intersection @Alive @Notified}
         if  Resurrected \= nil then                      %(R)Intersection with Notified,not Suspected 
              for Pbeer in Resurrected do
                 Notified := {PbeerList.remove Pbeer @Notified}
                 {@Listener alive(Pbeer)}
              end         
	      if @Period < MAX_TIMEOUT then
                  Period := @Period + DELTA
              end
         /*elseif @Period > TIMEOUT then
              Period := @Period - DELTA*/
         end
         
         Suspected = {PbeerList.minus @Pbeers @Alive}
         %% Only notify about new suspicions
         for Pbeer in {PbeerList.minus Suspected @Notified} do
            {@Listener crash(Pbeer)}
         end
         %% Clear up and get ready for new ping round
         Notified    := {PbeerList.union @Notified Suspected}
         Alive       := {PbeerList.new}
         Pbeers      := {PbeerList.union @Pbeers @NewPbeers}
         NewPbeers   := {PbeerList.new}

         %DebugVar    := @DebugVar + 1
         {NewRound start}
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
      NewPbeers   = {NewCell {PbeerList.new}}
      Alive       = {NewCell {PbeerList.new}} 
      Notified    = {NewCell {PbeerList.new}}
      Period      = {NewCell TIMEOUT}
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      %DebugVar    = {NewCell 0}

      Self        = {Component.new Events}
      Listener    = Self.listener
      {TheTimer setListener(Self.trigger)}
      
      {NewRound start}
      Self.trigger 
   end
end

