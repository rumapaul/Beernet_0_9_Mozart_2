/*-------------------------------------------------------------------------
 *
 * FingerTable.oz
 *
 *    K-ary finger table to route message in O(log_k(N)) hops.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 412 $ $Author: boriss $
 *
 *    $Date: 2011-05-29 23:24:05 +0200 (Sun, 29 May 2011) $
 *
 * NOTES
 *      
 *     This Finger Table is based on DKS generalization of Chord fingers to
 *     guarantee O(log_k(N)) hops (at the level of the overlay, not counting
 *     tcp/ip connections). The idea is that the address space of size N is
 *     divided in k, and then, the smallest fraction is divided again into k,
 *     until the granularity is small enough.
 *
 *     The Finger Table does not receives messages from the comunication
 *     layer. It only sends messages through it. Messages are receive by the
 *     Finger Table from within the node using the event route(...).
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../../corecomp/Component.ozf'
   KeyRanges   at '../../utils/KeyRanges.ozf'
   RingList    at '../../utils/RingList.ozf'
   Utils       at '../../utils/Misc.ozf'
export
   New
define

   %% Default values
   K_DEF    = 4         % Factor k for k-ary fingers

   fun {New Args}
      Self
      Id          % Id of the owner node. Pivot for relative ids
      Fingers     % RingList => sorted using Id first reference
      IdealIds    % Ideals ids to chose the fingers
      K           % Factor k to divide the address space to choose fingers
      MaxKey      % Maximum value for a key
      Node        % The Node that uses this finger table
      NodeRef     % Node's reference
      Refresh     % Flag to know if refreshing is finished
      Refreshing  % List of acknowledged ids

      ComLayer    % Communication Layer, to send messages.

      % --- Utils ---
      fun {CheckNewFinger Ids Fgs New}
         case Ids
         of H|T then
            if {RingList.isEmpty Fgs} then
               {RingList.add New Fgs @Id @MaxKey}
            else
               P  = {RingList.getFirst Fgs noFinger}
               Ps = {RingList.tail Fgs}
            in
               if {KeyRanges.checkOrder @Id H P.id} then
                  if {KeyRanges.checkOrder @Id H New.id} then
                     if {KeyRanges.checkOrder @Id New.id P.id} then
                        {RingList.add New {CheckNewFinger T Ps P} @Id @MaxKey}
                     else
                        {RingList.add P {CheckNewFinger T Ps New} @Id @MaxKey}
                     end
                  else
                     Fgs
                  end
               else
                  {CheckNewFinger Ids Ps New}
               end
            end
         [] nil then
            {RingList.new}
         end
      end

      fun {ClosestPrecedingFinger Key}
         {RingList.getBefore Key @Fingers @Id @MaxKey}
      end

      %% --- Events --- 
      proc {AddFinger addFinger(Pbeer)}
         Fingers := {CheckNewFinger @IdealIds @Fingers Pbeer}
      end

      proc {FindFingers Event}
         findFingers(_/*Contact*/) = Event
      in
         skip
      end

      proc {GetFingers getFingers(TheFingers)}
         TheFingers = @Fingers
      end

      proc {Monitor monitor(Pbeer)}
         Fingers := {CheckNewFinger @IdealIds @Fingers Pbeer}
      end

      proc {NeedFinger needFinger(src:Src key:K)}
         {@Node dsend(to:Src newFinger(key:K src:@NodeRef))}
      end

      proc {NewFinger newFinger(key:K src:Pbeer)}
         Fingers     := {CheckNewFinger @IdealIds @Fingers Pbeer}
         Refreshing  := {Utils.deleteFromList K @Refreshing} 
         if @Refreshing == nil then %% Got all refreshing fingers answers
            @Refresh = unit
         end
      end

      proc {RefreshFingers refreshFingers(Flag)}
         Refreshing  := @IdealIds
         @Refresh    = Flag
         for K in @IdealIds do
            {@Node route(msg:needFinger(src:@NodeRef key:K) src:@NodeRef to:K)}
         end
      end

      proc {RemoveFinger removeFinger(Finger)}
         Fingers := {RingList.remove Finger @Fingers}
      end

      proc {Route Event}
         route(msg:Msg src:Src to:Target ...) = Event
      in
         if {Not {Record.label Msg} == join} then
            {Monitor monitor(Src)}
         end
         {@ComLayer sendTo({ClosestPrecedingFinger Target} Event)}
      end

      proc {SetComLayer setComLayer(NewComLayer)}
         ComLayer := NewComLayer
      end

      proc {SetId setId(NewId)}
         Id := NewId
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      proc {SetK setK(NewK)}
         K := NewK
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      proc {SetMaxKey setK(NewMaxKey)}
         MaxKey := NewMaxKey
         IdealIds := {KeyRanges.karyIdFingers @Id @K @MaxKey}
      end

      Events = events(
                  addFinger:     AddFinger
                  findFingers:   FindFingers
                  getFingers:    GetFingers
                  monitor:       Monitor
                  needFinger:    NeedFinger
                  newFinger:     NewFinger
                  refreshFingers:RefreshFingers
                  removeFinger:  RemoveFinger
                  route:         Route
                  setComLayer:   SetComLayer
                  setId:         SetId
                  setMaxKey:     SetMaxKey
                  setK:          SetK
                  )
   in %% --- New starts ---
      Self        = {Component.newTrigger Events}
      K           = {NewCell K_DEF}
      Node        = {NewCell Args.node}
      MaxKey      = {NewCell {@Node getMaxKey($)}}
      Id          = {NewCell {@Node getId($)}}
      NodeRef     = {NewCell {@Node getRef($)}}
      IdealIds    = {NewCell {KeyRanges.karyIdFingers @Id @K @MaxKey}}
      Fingers     = {NewCell {RingList.new}}
      Refreshing  = {NewCell nil}
      Refresh     = {NewCell _}
      ComLayer    = {NewCell Component.dummy}
      Self
   end

end
