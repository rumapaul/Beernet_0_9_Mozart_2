/*-------------------------------------------------------------------------
 *
 * RelaxedRing.oz
 *
 *    Relaxed-ring maintenance algorithm
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
 *    Join and failure recovery are implemented here.
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
   System
   Component   at '../../corecomp/Component.ozf'
   Constants   at '../../commons/Constants.ozf'
   KeyRanges   at '../../utils/KeyRanges.ozf'   
   Network     at '../../network/Network.ozf'
   PbeerList   at '../../utils/PbeerList.ozf'
   RingList    at '../../utils/RingList.ozf'
   TimerMaker  at '../../timer/Timer.ozf'
   Utils       at '../../utils/Misc.ozf'
   %PeriodicStabilizer at 'PeriodicStabilizer.ozf'
   %Merger      at 'Merger.ozf'

export
   New
define
   JOIN_WAIT   = 5000      % Milliseconds to wait to retry a join 
   MAX_KEY     = Constants.largeKey
   SL_SIZE     = Constants.slSize
   IS_VISUAL_DEBUG = Constants.isVisual

   BelongsTo      = KeyRanges.belongsTo

   %% --- Utils ---
   fun {Vacuum L Dust}
      case Dust
      of DeadPeer|MoreDust then
         {Vacuum {RingList.remove DeadPeer L} MoreDust}
      [] nil then
         L
      end
   end

   %% --- Exported ---
   fun {New CallArgs}
      Crashed     % List of crashed peers
      MaxKey      % Maximum value for a key
      Pred        % Reference to the predecessor
      PredList    % To remember peers that haven't acked joins of new preds
      Ring        % Ring Reference ring(name:<atom> id:<name>)
      FingerTable % Routing table 
      %SelfStabilizer  % Periodic Stalizer
      %SelfMerger  % Merge Module
      Self        % Full Component
      SelfRef     % Pbeer reference pbeer(id:<Id> port:<Port>)
      Succ        % Reference to the successor
      SuccList    % Successor List. Used for failure recovery
      SLSize      % Successor list size
      WishedRing  % Used while trying to join a ring

      %% --- Utils ---
      ComLayer    % Network component
      Listener    % Component where the deliver messages will be triggered
      Logger      % Component to log every sent and received message  (R)
      Timer       % Component to rigger some events after the requested time

      Args
      FirstAck    % One shoot acknowledgement for first join

      fun {AddToList Peer L}
         {RingList.add Peer L @SelfRef.id MaxKey}
      end

      %% TheList should have no more than Size elements
      fun {UpdateList MyList NewElem OtherList}
         FinalList _/*DropList*/
      in
         FinalList = {NewCell MyList}
         {RingList.forAll {Vacuum {AddToList NewElem OtherList} @Crashed}
                           proc {$ Pbeer}
                              FinalList := {AddToList Pbeer @FinalList}
                           end}
         FinalList := {RingList.keepAndDrop SLSize @FinalList _/*DropList*/}
         % TODO: verify this operation
         %{UnregisterPeers DropList}
         {WatchPeers @FinalList}
         @FinalList
      end

      proc {BasicForward Event}
         case Event
         of route(msg:Msg src:_ to:_) then
            if @Succ \= nil then
               {Zend @Succ Msg}
            end
         else
            skip
         end
      end

      proc {Backward Event Target}
         ThePred = {RingList.getAfter Target @PredList @SelfRef.id MaxKey}
      in
         if ThePred \= nil then
            {Zend ThePred Event}
         %else
            %%TODO: acknowledge somehow that the message is lost
         %   {System.showInfo "Something went wrong, I cannot backward msg"}
         %   skip
         end
      end

     proc {BackwardForPreceedingId Event Target}
         ThePred = {RingList.getBefore Target @PredList @SelfRef.id MaxKey}
      in
         if ThePred \= nil then
            {Zend ThePred Event}
         %else
            %%TODO: acknowledge somehow that the message is lost
         %   {System.showInfo "Something went wrong, I cannot backward msg"}
         %   skip
         end
      end

%      fun {GetNewPBeerRef}
%         pbeer(id:{KeyRanges.getRandomKey MaxKey}
%               port:{@ComLayer getPort($)})
%      end

      %% Registering a Pbeer on the failure detector
      proc {Monitor Pbeer}
         {@ComLayer monitor(Pbeer)}
      end

      proc {RlxRoute Event Target}
         if {HasFeature Event last} andthen Event.last then
            %% I am supposed to be the responsible, but I have a branch
            %% or somebody was missed (non-transitive connections)
            {Backward Event Target}
         elseif {BelongsTo Event.src.id @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible => set last = true
            {Zend @Succ {Record.adjoinAt Event last true}}
            %{Blabla @SelfRef.id#" forwards join of "#Sender.id#" to "
            %         #@(Self.succ).id}
         else
            %% Forward the message using the routing table
            {@FingerTable route(msg:Event src:Event.src to:Target)}
            %{Blabla @SelfRef.id#" forwards join of "#Src.id}
         end
      end

      proc {ClosestPreceedingRoute Event Target}
         if {HasFeature Event last} andthen Event.last then
            %% I am supposed to be the responsible, but I have a branch
            %% or somebody was missed (non-transitive connections)
            {BackwardForPreceedingId Event Target}
         elseif {BelongsTo Event.src.id @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible => set last = true
            {Zend @Succ {Record.adjoinAt Event last true}}
         else
            %% Forward the message using the routing table
            {@FingerTable route(msg:Event src:Event.src to:Target)}
         end
      end

      proc {Update CandidatePbeer}
        if {Not {PbeerList.isIn CandidatePbeer @Crashed}} then
            %{System.showInfo "At:"#@Pred.id#"->"#@SelfRef.id#"->"#
            %                         @Succ.id#" in Update:"#CandidatePbeer.id}
            if {BelongsTo CandidatePbeer.id @SelfRef.id @Succ.id-1} then
                OldSucc = @Succ.id
                in
                %{System.showInfo "At:"#@SelfRef.id#" in Update New Succ:"#CandidatePbeer.id#" Old Succ:"#@Succ.id}
	        Succ := CandidatePbeer
                {Monitor CandidatePbeer}
                {@FingerTable monitor(CandidatePbeer)}
                %{Zend @Succ fix(src:@SelfRef)}
                if IS_VISUAL_DEBUG == 1 then
                     {@Logger event(@SelfRef.id succChanged(@Succ.id OldSucc) color:green)}
                end
            elseif {BelongsTo CandidatePbeer.id @Pred.id @SelfRef.id-1} then
                OldPred = @Pred
                in
                 %{System.showInfo "At:"#@SelfRef.id#" in Update New Pred:"#CandidatePbeer.id#" Old Pred:"#@Pred.id}
                 PredList := {AddToList CandidatePbeer @PredList}
                 Pred := CandidatePbeer 
                 {Monitor CandidatePbeer}
                 {@FingerTable monitor(CandidatePbeer)}
		 %% Tell data management to migrate data in range ]OldPred, Pred]
            	 {@Listener newPred(old:OldPred new:@Pred tag:data)}
                 if IS_VISUAL_DEBUG == 1 then
                      {@Logger event(@SelfRef.id predChanged(@Pred.id OldPred.id) color:darkblue)}
                      {@Logger event(@SelfRef.id onRing(true) color:darkblue)} %TODO: Check
                 end
            end
        end 
      end

      proc {WatchPeers Peers}
         {RingList.forAll Peers
            proc {$ Peer}
               {@ComLayer monitor(Peer)}
            end}
      end

      proc {Zend Target Msg}
         %{System.show @SelfRef.id#'sending a darn message'#Msg#to#Target.id}
         {@ComLayer sendTo(Target Msg log:rlxring)}
      end

      %%--- Events ---

      proc {Alive alive(Pbeer)}
         Crashed  := {PbeerList.remove Pbeer @Crashed}
         %if IS_VISUAL_DEBUG == 1 then
         %     {@Logger event(@SelfRef.id alive(Pbeer.id) color:green)}
         %end
         %{@Logger stat(src:@SelfRef.id msg:alive pointer:Pbeer.id)}
         {@FingerTable monitor(Pbeer)}
         if {BelongsTo Pbeer.id @Pred.id @SelfRef.id-1} then
            OldPred = @Pred
            in
            PredList := {AddToList Pbeer @PredList}
            Pred := Pbeer %% Monitoring Pbeer and it's on predList
            %% Tell data management to migrate data in range ]OldPred, Pred]
            {@Listener newPred(old:OldPred new:@Pred tag:data)}
            
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id predChanged(@Pred.id OldPred.id) color:darkblue)}
            %     {@Logger event(@SelfRef.id onRing(true) color:darkblue)} %TODO:Not always correct
            %end
            %{@Logger stat(src:@SelfRef.id new:@Pred.id msg:inconsistency pointer:pred)}
         elseif {BelongsTo Pbeer.id @SelfRef.id @Succ.id-1} then
            %OldSucc = @Succ.id
            %in   
            Succ := Pbeer
            {Zend @Succ fix(src:@SelfRef)}
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id succChanged(@Succ.id OldSucc) color:green)}
            %end
            %{@Logger stat(src:@SelfRef.id new:@Succ.id msg:inconsistency pointer:succ)}
         %else
         %   {MakeAQueueInsert makeAQueueInsert(Pbeer)}
         end 
      end

      proc {Any Event}
         %{System.show '++++++++++triggering Event to Listener'#Event}
         %{System.show 'Listener'#@Listener}
         {@Listener Event}
      end

      proc {BadRingRef Event}
         badRingRef = Event
      in
         {System.show 'BAD ring reference. I cannot join'}
         skip %% TODO: trigger some error message
      end

      proc {Crash crash(Pbeer)}
         Crashed  := {PbeerList.add Pbeer @Crashed}
         SuccList := {RingList.remove Pbeer @SuccList}
         PredList := {RingList.remove Pbeer @PredList}
	 %{@Listener nodeCrash(node:Pbeer tag:trapp)}
         {@FingerTable removeFinger(Pbeer)}
         %{@Listener nodeCrash(old:Pbeer.id new:@Pred.id tag:trapp)}
         %{@Logger stat(src:@SelfRef.id msg:crash pointer:Pbeer.id)}
         %if IS_VISUAL_DEBUG == 1 then
         %     {@Logger event(@SelfRef.id crash(Pbeer.id) color:red)}
         %end
         if Pbeer.id == @Succ.id then
            Succ := {RingList.getFirst @SuccList @SelfRef}
            {Monitor @Succ}
            {Zend @Succ fix(src:@SelfRef)}
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id succChanged(@Succ.id Pbeer.id) color:green)}
            %end
            %{@Logger stat(src:@SelfRef.id msg:connsuspicion suspected:Pbeer.id pointer:succ)}
         end
         if Pbeer.id == @Pred.id then
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id onRing(false) color:darkblue)}
            %end
            if @PredList \= nil then
               %Pred := {RingList.getLast @PredList @Pred}
               Pred := {RingList.getLast @PredList @SelfRef}
               {Monitor @Pred}
               %% Tell data management to migrate data in range ]Pred, OldPred]
               {@Listener newResponsibilities(old:Pbeer.id new:@Pred.id tag:trapp)}
	       %if IS_VISUAL_DEBUG == 1 then
               %     {@Logger event(@SelfRef.id predChanged(@Pred.id Pbeer.id) color:darkblue)}
               %end
               %{@Logger stat(src:@SelfRef.id msg:connsuspicion suspected:Pbeer.id pointer:pred)}
            end
         end
      end

      %% DSend
      %% Send directly to a port with the correct format
      proc {DSend Event}
         Msg = Event.1
         To = Event.to
      in
         if {HasFeature Event log} then   
            {@ComLayer sendTo(To Msg log:Event.log)}
         else
            {@ComLayer sendTo(To Msg)}
         end
      end

      %%% Midnattsol
      %% Fix means 'Self is Src's new succ' and 'Src wants to be Self's pred'
      %% Src is accepted as predecessor if:
      %% 1 - the current predecessor is dead
      %% 2 - Src is in (pred, self]
      %% Otherwise is a better predecessor of pred.
      proc {Fix fix(src:Src)}
	 OldPred = @Pred
	 in
         %% Src thinks I'm its successor so I add it to the predList
         PredList := {AddToList Src @PredList}
         {Monitor Src}
         if {PbeerList.isIn @Pred @Crashed} then
            Pred := Src %% Monitoring Src already and it's on predList
            {Zend Src fixOk(src:@SelfRef succList:@SuccList)}
            {Monitor Src}
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id predChanged(Src.id OldPred.id) color:darkblue)}
            %     {@Logger event(@SelfRef.id onRing(true) color:darkblue)}
            %end
            %% Tell data management to migrate data in range ]Pred, OldPred]
            {@Listener newResponsibilities(old:OldPred.id new:@Pred.id tag:trapp)}
         elseif {BelongsTo Src.id @Pred.id @SelfRef.id-1} then
            Pred := Src %% Monitoring Src already and it's on predList
            {Zend Src fixOk(src:@SelfRef succList:@SuccList)}
            {Monitor Src}
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id predChanged(Src.id OldPred.id) color:darkblue)}
            %     {@Logger event(@SelfRef.id onRing(true) color:darkblue)}
            %end
            %% Tell data management to migrate data in range ]OldPred, Pred]
            {@Listener newPred(old:OldPred new:@Pred tag:data)}
         else
            %{System.show 'GGGGGGGGGGRRRRRRRRRRRAAAAAAAAAA'#@SelfRef.id}
            %% Just keep it in a branch
            %{RlxRoute predFound(pred:Src last:true) Src.id Self}
            skip
         end
      end

      proc {FixOk fixOk(src:Src succList:SrcSuccList)}
         SuccList := {UpdateList @SuccList Src SrcSuccList}
         {Zend @Pred updSuccList(src:@SelfRef
                                 succList:@SuccList
                                 counter:SLSize)}
      end

      proc {GetComLayer getComLayer(Res)}
         Res = @ComLayer
      end

      proc {GetFullRef getFullRef(FullRef)}
         FullRef = ref(pbeer:@SelfRef ring:@Ring)
      end

      proc {GetId getId(Res)}
         Res = @SelfRef.id
      end

      proc {GetMaxKey getMaxKey(Res)}
         Res = MaxKey
      end

      proc {GetPred getPred(Peer)}
         Peer = @Pred
      end

      proc {GetRange getRange(Res)}
      	Res = (@Pred.id+1 mod MaxKey)#@SelfRef.id
      end

      proc {GetRef getRef(Res)}
      	Res = @SelfRef
      end

      proc {GetRingRef getRingRef(RingRef)}
         RingRef = @Ring
      end

      proc {GetSucc getSucc(Peer)}
         Peer = @Succ
      end

      proc {Hint hint(succ:NewSucc)}
         if {BelongsTo NewSucc.id @SelfRef.id @Succ.id-1} then
            {Zend @Succ predNoMore(@SelfRef)}
            Succ := NewSucc
            {Monitor NewSucc}
            {@FingerTable monitor(NewSucc)}
            {Zend @Succ fix(src:@SelfRef)}
         end
      end

      proc {IdInUse idInUse(Id)}
         %%TODO. Get a new id and try to join again
         if @SelfRef.id == Id then
            {System.show 'I cannot join because my Id is already in use'}
         else
            {System.showInfo '#'("My id " @SelfRef.id
                                 " is considered to be in use as " Id)}
         end
      end

      proc {Init Event}
         skip
      end

      proc {Join Event}
         Src = Event.src
 	 SrcRing = Event.ring 
         %% Event join might come with flag last = true guessing to reach the
         %% responsible. If I am not the responsible, message has to be 
         %% backwarded to the branch. 
      in
         if @Ring \= SrcRing then
            {Zend Src badRingRef}
         elseif @SelfRef.id == Src.id then
            {Zend Src idInUse(Src.id)}
         elseif {Not {PbeerList.isIn @Succ @Crashed}} then
            if {BelongsTo Src.id @Pred.id @SelfRef.id} then
               OldPred = @Pred
            in
               {Zend Src joinOk(pred:OldPred
                                succ:@SelfRef
                                succList:@SuccList)}
               Pred := Src
               {Monitor Src} 
               if @PredList \= nil then		%% Code to prune branch
                  CloserPeerOnBranch = {RingList.getLast @PredList nil}
                  in
                  if CloserPeerOnBranch\=nil then
                    {Zend CloserPeerOnBranch hint(succ:Src)}
                  end
               end     
               
               PredList := {AddToList @Pred @PredList}
               %% Tell data management to migrate data in range ]OldPred, Pred]
               {@Listener newPred(old:OldPred new:@Pred tag:data)}
               %if IS_VISUAL_DEBUG == 1 then
               %     {@Logger event(@SelfRef.id predChanged(@Pred.id OldPred.id) color:darkblue)}
               %end
            else
               %NOT FOR ME - going to route
               {RlxRoute Event Src.id}
            end
         else
            {Zend Src joinLater}
         end
      end

      proc {PredNoMore predNoMore(OldPred)}
         PredList := {RingList.remove OldPred @PredList}
         %% TODO: Add treatment of hint message here
      end

      proc {JoinLater joinLater(NewSucc)}
         %{Timer JOIN_WAIT Self startJoin(succ:NewSucc ring:@WishedRing)}
         {Timer startTrigger(JOIN_WAIT startJoin(succ:NewSucc ring:@WishedRing) Self)}
      end

      proc {JoinOk joinOk(pred:NewPred succ:NewSucc succList:NewSuccList)}
         if {BelongsTo NewSucc.id @SelfRef.id @Succ.id} then
            Succ := NewSucc
            SuccList := {UpdateList @SuccList NewSucc NewSuccList}
            Ring := @WishedRing
            WishedRing := none
            {Monitor NewSucc} 
            {@FingerTable monitor(NewSucc)}
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
            FirstAck = unit
            %if IS_VISUAL_DEBUG == 1 then
	    %     {@Logger event(@SelfRef.id newSucc(@Succ.id) color:green)}
            %end
         end
         if {BelongsTo NewPred.id @Pred.id @SelfRef.id} then
            {Zend NewPred newSucc(newSucc:@SelfRef succList:@SuccList)}
            Pred := NewPred
            PredList := {AddToList NewPred @PredList}
            %% set a failure detector on the predecessor
            {Monitor NewPred} 
            {@FingerTable monitor(NewPred)}
            %if IS_VISUAL_DEBUG == 1 then
            %     {@Logger event(@SelfRef.id newPred(@Pred.id) color:green)}
            %end
         end
      end

      proc {Lookup lookup(key:Key res:Res)}
         HKey
      in
         HKey = {Utils.hash Key MaxKey}
         {LookupHash lookupHash(hkey:HKey res:Res)}
      end

      proc {LookupHash lookupHash(hkey:HKey res:Res)}
         {Route route(msg:lookupRequest(res:Res) src:@SelfRef to:HKey)}
      end

      proc {LookupRequest lookupRequest(res:Res)}
         %% TODO: mmm... can we trust distributed variable binding?
         Res = @SelfRef
      end

     proc {MakeAQueueInsert Event}
         %{SelfMerger Event}
	skip
     end

     proc {MLookup Event}
         Target = Event.id
         F = Event.fanout
         in
         if F > 1 then
            skip %% I'll do something
         end
  
         if Target \= @SelfRef andthen Target \= @Succ then
            if {BelongsTo Target.id @SelfRef.id @Succ.id} then
               {Zend Target retrievePred(src:@SelfRef psucc:@Succ)}
            else
               {ClosestPreceedingRoute Event Target.id}
            end
         end
         {Update Target}
         {System.showInfo "In MLookup Merger:"#Target.id#" Fanout:"#F}
      end

      proc {NewSucc newSucc(newSucc:NewSucc succList:NewSuccList)}
	 %OldSucc = @Succ.id
	 %in
         if {BelongsTo NewSucc.id @SelfRef.id @Succ.id} then
            SuccList := {UpdateList @SuccList NewSucc NewSuccList}
            {Zend @Succ predNoMore(@SelfRef)}
            {Zend @Pred updSuccList(src:@SelfRef
                                    succList:@SuccList
                                    counter:SLSize)}
            Succ := NewSucc
            {Monitor NewSucc}
            {@FingerTable monitor(NewSucc)}
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
            %if IS_VISUAL_DEBUG == 1 then
	    %     {@Logger event(@SelfRef.id succChanged(@Succ.id OldSucc) color:green)}
	    %     {@Logger event(@Succ.id onRing(true) color:darkblue)}
            %end
         end
      end

      proc {RetrievePred retrievePred(src:Src psucc:PSucc)}
 	 {Zend Src retrievePredRes(src:@SelfRef
                                  succp:@Pred
                                  succList:@SuccList)}
         if PSucc.id \= @SelfRef.id then
               {MakeAQueueInsert makeAQueueInsert(PSucc)}
         end
         {Update Src}
      end

      proc {RetrievePredRes retrievePredRes(src:Src succp:SuccP succList:SuccSL)}
         {Update SuccP}
         {UpdSuccList updSuccList(src:Src succList:SuccSL counter:SLSize)}
      end

      proc {Route Event}
         Msg = Event.msg
	 Target = Event.to
      in
         %{System.show 'going to route '#Msg}
         if {BelongsTo Target @Pred.id @SelfRef.id} then
            %{System.show @SelfRef.id#' it is mine '#Event}
            %% This message is for me
            {Self Msg}
         elseif {HasFeature Event last} andthen Event.last then
            %{System.show @SelfRef.id#' gotta go backwards '#Event}
            %% I am supposed to be the responsible, but I have a branch
            %% or somebody was missed (non-transitive connections)
            {Backward Event Target}
         elseif {BelongsTo Target @SelfRef.id @Succ.id} then
            %% I think my successor is the responsible => set last = true
            %{System.show @SelfRef.id#' I missed one week? '#Event}
            {Zend @Succ {Record.adjoinAt Event last true}}
            %{Blabla @SelfRef.id#" forwards join of "#Sender.id#" to "
            %         #@(Self.succ).id}
         else
            %% Forward the message using the routing table
            %{System.show @SelfRef.id#'Forwarding '#Event}
            {@FingerTable Event}
            %{Blabla @SelfRef.id#" forwards join of "#Src.id}
         end
      end

      proc {SetFingerTable setFingerTable(NewFingerTable)}
         FingerTable := NewFingerTable
      end

      proc {SetLogger Event}
	setLogger(NewLogger) = Event
      in
	Logger := NewLogger
         %{@ComLayer Event}
	 {@ComLayer setLogger(NewLogger)}
      end

      proc {Stabilize stabilize}
         {Zend @Succ retrievePred(src:@SelfRef
                                  psucc:@Succ)}
      end

      proc {StartJoin startJoin(succ:NewSucc ring:RingRef)}
         WishedRing := RingRef
         {Zend NewSucc join(src:@SelfRef ring:RingRef)}
      end

      proc {UpdSuccList Event}
         updSuccList(src:Src succList:NewSuccList counter:Counter) = Event
      in
         if @Succ.id == Src.id then
            SuccList := {UpdateList @SuccList Src NewSuccList}
            if Counter > 0 then
               {Zend @Pred updSuccList(src:@SelfRef
                                       succList:@SuccList
                                       counter:Counter - 1)}
            end
            {RingList.forAll @SuccList proc {$ Pbeer}
                                          {@FingerTable monitor(Pbeer)}
                                       end}
         end
      end

      ToFingerTable = {Utils.delegatesTo FingerTable}

      Events = events(
                  alive:         Alive
                  any:           Any
                  crash:         Crash
                  badRingRef:    BadRingRef
                  dsend:         DSend
                  fix:           Fix
                  fixOk:         FixOk
                  getComLayer:   GetComLayer
                  getFullRef:    GetFullRef
                  getId:         GetId
                  getMaxKey:     GetMaxKey
                  getPred:       GetPred
                  getRange:      GetRange
                  getRef:        GetRef
                  getRingRef:    GetRingRef
                  getSucc:       GetSucc
                  hint:          Hint
                  idInUse:       IdInUse
                  init:          Init
                  join:          Join
                  joinLater:     JoinLater
                  joinOk:        JoinOk
                  lookup:        Lookup
                  lookupHash:    LookupHash
                  lookupRequest: LookupRequest
                  mlookup:       MLookup
                  makeAQueueInsert: MakeAQueueInsert
                  needFinger:    ToFingerTable
                  newFinger:     ToFingerTable
                  newSucc:       NewSucc
                  predNoMore:    PredNoMore
                  route:         Route
                  refreshFingers:ToFingerTable
                  retrievePred:  RetrievePred
                  retrievePredRes:RetrievePredRes 
                  setFingerTable:SetFingerTable
                  setLogger:     SetLogger
                  stabilize:     Stabilize
                  startJoin:     StartJoin
                  updSuccList:   UpdSuccList
                  )

   in %% --- New starts ---
      %% Creating the component and collaborators
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Listener = FullComponent.listener
      end
      Timer = {TimerMaker.new}
      ComLayer = {NewCell {Network.new}}
      {@ComLayer setListener(Self)}

      Logger      = {NewCell Component.dummy}	%(R)

      Args        = {Utils.addDefaults CallArgs
                                       def(firstAck:_
                                           maxKey:MAX_KEY
                                           slsize:SL_SIZE)}
      FirstAck    = Args.firstAck
      MaxKey      = Args.maxKey
      SLSize      = Args.slsize

      %% Peer State
      if {HasFeature Args id} then
         SelfRef = {NewCell pbeer(id:Args.id)}
      else
         SelfRef = {NewCell pbeer(id:{KeyRanges.getRandomKey MaxKey})}
      end
      SelfRef := {Record.adjoinAt @SelfRef port {@ComLayer getPort($)}}
      {@ComLayer setId(@SelfRef.id)}
      
      if {HasFeature Args fdParams} then
         {@ComLayer setFDParams(Args.fdParams)}
      end

      %SelfStabilizer = {PeriodicStabilizer.new}
      %{SelfStabilizer setComLayer(@ComLayer)}
      %{SelfStabilizer setListener(Self)}


      Pred        = {NewCell @SelfRef}
      Succ        = {NewCell @SelfRef}
      PredList    = {NewCell {RingList.new}}
      SuccList    = {NewCell {RingList.new}} 
      Crashed     = {NewCell {PbeerList.new}}
      Ring        = {NewCell ring(name:lucifer id:{Name.new})}
      WishedRing  = {NewCell none}
      FingerTable = {NewCell BasicForward}

      %% For ReCircle
      %SelfMerger = {Merger.new}
      %{SelfMerger setComLayer(@ComLayer)}
      %{SelfMerger setListener(Self)}
     
      %% Return the component
      Self
   end
end
