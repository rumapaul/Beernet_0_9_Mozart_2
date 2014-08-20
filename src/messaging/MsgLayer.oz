/*-------------------------------------------------------------------------
 *
 * MsgLayer.oz
 *
 *    Messaging layer that uses any ring-based node to perform reliable
 *    message sending routing through the overlay network.
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
 *    The messaging layer needs a Node to route and receive messages. It also
 *    needs to be registered as listener of the Node, in other case, messages
 *    will not be delivered to the upper layer.
 *    
 *-------------------------------------------------------------------------
 */

functor
import
   %System
   Component   at '../corecomp/Component.ozf'
   Timer       at '../timer/Timer.ozf'
   Utils       at '../utils/Misc.ozf'
export
   New
define
   
   fun {New CallArgs}
      Self
      Listener
      Node
      NodeRef
      TheTimer

      Args
      LastMsgId
      Msgs
      Timeout
      Tries

      fun {GetNewMsgId}
         OutId NewId
      in
         OutId = LastMsgId := NewId
         NewId = OutId + 1
         OutId
      end

      %% --- Events ---

      proc {DMsg dmsg(Msg)}
         {@Listener Msg}
      end

      proc {DSend dsend(Msg to:To)}
         {@Node dsend(dmsg(Msg) to:To)}
      end

      proc {Send Event}
         Msg = Event.1
         Target = Event.to
         %% Other args: responsible (resp), outcome (out)
         Resp
         Outcome
         FullMsg
         MsgId
      in
         %{System.show 'MsgLayer is going to send :'#Msg}
         if {HasFeature Event resp} then
            Resp = Event.resp
         else
            Resp = true
         end
         if {HasFeature Event out} then
            Outcome = Event.out
         end
         MsgId = {GetNewMsgId}
         FullMsg = rsend(msg:Msg to:Target src:@NodeRef resp:Resp mid:MsgId)
         Msgs.MsgId := data(msg:FullMsg outcome:Outcome c:@Tries)
         {@Node route(msg:FullMsg to:Target src:@NodeRef)} 
         {TheTimer startTrigger(@Timeout timeout(MsgId) Self)}
      end

      proc {RSend rsend(msg:Msg to:Target src:Src resp:Resp mid:MsgId)}
         %{System.show '+_+_MAYBE+_+_+_+_+_+_+_+_+_+got a message '#Msg}
         if Resp orelse Target == @NodeRef.id then
            %{System.show '+_+_+_+_+_+_+_+_+_+_+_+got a message '#Msg}
            {@Listener Msg}
            {@Node dsend(to:Src rsendAck(MsgId))}
         end
      end

      proc {RSendAck rsendAck(MsgId)}
         Data = {Dictionary.condGet Msgs MsgId done}
      in
         if Data \= done then
            Data.outcome = true
            %{System.show 'Msg'#MsgId#' was correctly received'}
            {Dictionary.remove Msgs MsgId}
         end
      end

      proc {SetNode setNode(ANode)}
         Node     := ANode
         NodeRef  := {@Node getRef($)}
      end

      proc {SetRef setRef(ARef)}
         NodeRef := ARef
      end

      proc {TimeoutEvent timeout(MsgId)}
         Data = {Dictionary.condGet Msgs MsgId done} 
      in
         if Data \= done then
            if Data.c > 1 then
               {@Node route(msg:Data.msg to:Data.msg.to src:@NodeRef)} 
               {TheTimer startTrigger(@Timeout timeout(MsgId) Self)}
               Msgs.MsgId := {Record.adjoinAt Data c Data.c-1}
            else
               %{System.show 'msg '#MsgId#':'#{Label Data.msg}#' never arrived'}
               Data.outcome = false
               {Dictionary.remove Msgs MsgId}
            end
         end
      end

      ToListener  = {Utils.delegatesTo Listener}
      ToNode      = {Utils.delegatesTo Node}

      Events = events(
                     any:        ToListener
                     rsend:      RSend
                     rsendAck:   RSendAck
                     dmsg:       DMsg
                     dsend:      DSend
                     getRef:     ToNode
                     send:       Send
                     setNode:    SetNode
                     setRef:     SetRef
                     timeout:    TimeoutEvent
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Listener = FullComponent.listener
      end
      Node        = {NewCell Component.dummy}
      NodeRef     = {NewCell noref}
      TheTimer    = {Timer.new}
      Msgs        = {Dictionary.new}
      LastMsgId   = {NewCell 0}

      Args        = {Utils.addDefaults CallArgs def(timeout:1000 tries:5)}
      Timeout     = {NewCell Args.timeout}
      Tries       = {NewCell Args.tries}

      Self
   end

end
