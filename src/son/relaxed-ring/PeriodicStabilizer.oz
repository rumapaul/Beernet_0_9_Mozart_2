/*-------------------------------------------------------------------------
 *
 * PeriodicStabilizer.oz
 *
 *    Periodic Stabilizer for Beernet
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 1 $ $Author: ruma $
 *
 *    $Date: 2013-06-26 15:22:21 +0200 (Wed, 26 June 2013) $
 *
 * NOTES
 *      
 *    Periodically asks successor about it's predecessor, and triggers update in case
 *    any issue is found in neighbourhood.
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
   Component   at '../../corecomp/Component.ozf'
   Timer       at '../../timer/Timer.ozf'
export
   New

define

   DELTA       = 5000    % Granularity to trigger stabilization

   fun {New}
      ComLayer    % Low level communication layer
      Listener    % Component where the deliver messages will be triggered
      Self        % Reference to this component
      SelfPbeer   % Pbeer reference assinged by a external component

      TheTimer    % Component that triggers timeout

      %% Launch the timer for every period
      proc {NewPeriod start}
         {TheTimer startTimer(DELTA)}
      end

      proc {Timeout timeout}
         {@Listener stabilize}
         %{Delay 1000} 
         {NewPeriod start}
      end

      proc {SetPbeer setPbeer(NewPbeer)}
         SelfPbeer := NewPbeer
      end

      proc {SetComLayer setComLayer(TheComLayer)}
         ComLayer = TheComLayer
         SelfPbeer := {ComLayer getRef($)} 
      end

      Events = events(
                  setPbeer:      SetPbeer
                  setComLayer:   SetComLayer
                  start:         NewPeriod
                  timeout:       Timeout
                  )
   in
      SelfPbeer   = {NewCell pbeer(id:~1 port:_)}
      TheTimer    = {Timer.new}

      Self        = {Component.new Events}
      Listener    = Self.listener
   
      {TheTimer setListener(Self.trigger)}
      
      {NewPeriod start}
      Self.trigger 
   end
end

