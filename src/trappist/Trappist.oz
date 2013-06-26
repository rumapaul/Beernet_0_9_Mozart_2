/*-------------------------------------------------------------------------
 *
 * Trappist.oz
 *
 *    Interface to the different strategies for transactional storage
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 414 $ $Author: boriss $
 *
 *    $Date: 2011-05-30 15:30:05 +0200 (Mon, 30 May 2011) $
 *
 * NOTES
 *    
 *    Pre-condition: It needs a messaging layer, the DHT component, a
 *    replication manager and the Node Reference
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component      at '../corecomp/Component.ozf'
   Constants      at '../commons/Constants.ozf'
   Utils          at '../utils/Misc.ozf'
   EagerPaxosTM   at 'eagerpaxos/EagerPaxos-TM.ozf'
   EagerPaxosTP   at 'eagerpaxos/EagerPaxos-TP.ozf'
   PaxosTM        at 'paxos/Paxos-TM.ozf'
   PaxosTP        at 'paxos/Paxos-TP.ozf'
   TwoPhaseTM     at 'twophase/TwoPhase-TM.ozf'
   TwoPhaseTP     at 'twophase/TwoPhase-TP.ozf'
   ValueSetTM     at 'valueset/ValueSet-TM.ozf'
   ValueSetTP     at 'valueset/ValueSet-TP.ozf'
export
   New
define

   Debug = Utils.blabla

   fun {New CallArgs}
      Self
      %Listener
      MsgLayer
      NodeRef
      Replica

      %Timeout
      TMs
      TPs
      DBMan
      PairsDB
      SetsDB
      MaxKey

      TMmakers = tms(eagerpaxos: EagerPaxosTM
                     paxos:      PaxosTM
                     twophase:   TwoPhaseTM
                     valueset:   ValueSetTM
                     )
      TPmakers = tms(eagerpaxos: EagerPaxosTP
                     paxos:      PaxosTP
                     twophase:   TwoPhaseTP
                     valueset:   ValueSetTP
                     )

      DBs      = dbs(eagerpaxos: PairsDB
                     paxos:      PairsDB
                     twophase:   PairsDB
                     valueset:   SetsDB
                     )

      proc {AddTransObj TransDict Tid ObjId Obj}
         TheObjs
      in
         TheObjs = {Dictionary.condGet TransDict Tid objs}
         TransDict.Tid := {Record.adjoinAt TheObjs ObjId Obj}
      end

      %% === Events =========================================================

      %% --- Trappist API ---------------------------------------------------

      proc {BecomeReader Event}
         skip
      end

      proc {GetLocks Event}
         skip
      end

      %% Just a forward to runTransaction.
      %% Keeps backward compatibility
      proc {ExecuteTransaction executeTransaction(Trans Client Protocol)}
         {RunTransaction runTransaction(Trans Client Protocol)}
      end

      proc {RunTransaction runTransaction(Trans Client Protocol)}
         TM
      in
         TM = {TMmakers.Protocol.new args(role:leader
                                          client:Client
                                          maxKey:@MaxKey)}
         {TM setMsgLayer(@MsgLayer)}
         {TM setReplica(@Replica)}
         {Debug 'Trap: TM object created and ready to call the transaciton'}
         {AddTransObj TMs {TM getTid($)} {TM getId($)} TM}
         {Trans TM}
      end

      %% --- Trappist API for Key/Value-Sets --------------------------------
      %% Slightly different that Run Transaction
      %% Event can be:
      %% add(k:SetKey s:SetSecret v:Value sv:ValueSecret c:ClientP)
      %% remove(k:SetKey s:SetSecret v:Value sv:ValueSecret c:ClientP)
      %% readSet(k:SetKey v:Value)
      proc {ToValueSet Event}
         TM 
      in
         TM = {TMmakers.valueset.new args(role:leader maxKey:@MaxKey)}
         {TM setMsgLayer(@MsgLayer)}
         {TM setReplica(@Replica)}
         {AddTransObj TMs {TM getTid($)} {TM getId($)} TM}
         {TM Event}
      end

      %% --- For the TMs ----------------------------------------------------
      proc {InitRTM Event}
         initRTM(client:Client protocol:Protocol tid:Tid ...) = Event
         RTM
      in
         if @NodeRef.id \= Event.leader.ref.id then
            RTM = {TMmakers.Protocol.new args(role:rtm
                                              client:Client
                                              maxKey:@MaxKey)}
            {RTM setMsgLayer(@MsgLayer)}
            {RTM setReplica(@Replica)}
            {AddTransObj TMs Tid {RTM getId($)} RTM}
            {RTM Event}
         end
      end

      proc {ForwardToTM Event}
         {TMs.(Event.tid).(Event.tmid) Event}
      end 

      %% --- For the TPs ----------------------------------------------------
      proc {Brew Event}
         brew(tid:Tid protocol:Protocol ...) = Event
         TP
      in
         TP = {TPmakers.Protocol.new args(tid:Tid)} 
         {TP setMsgLayer(@MsgLayer)}
         {TP setDB(DBs.Protocol)}
         {AddTransObj TPs Tid {TP getId($)} TP}
         {TP Event}
      end

      proc {Final Event}
         {TPs.(Event.tid).(Event.tpid) Event.decision}
      end

      %% --- Data Management ------------------------------------------------
      proc {NewPred newPred(old:OldPred new:NewPred tag:data)}
         proc {DataLoop Froms Tos}
            case Froms#Tos
            of (From|MoreFroms)#(To|MoreTos) then
               PairEntries SetEntries
            in
               %% Migrating data of paxos, eagerpaxos and twophase
               %% They all use PairsDB, refering with DBs.paxos
               {DBs.paxos dumpRange(From To PairEntries)}
               {@MsgLayer dsend(to:NewPred insertData(entries:PairEntries
                                                      db:paxos
                                                      tag:trapp))}
               %% Migrating data of the valueset abstraction
               {DBs.valueset dumpRange(From To SetEntries)}
               {@MsgLayer dsend(to:NewPred insertData(entries:SetEntries
                                                      db:valueset
                                                      tag:trapp))}
               %% Carry on with the migration
               {DataLoop MoreFroms MoreTos}
            [] nil#nil then
               skip
            end
         end
         FromList
         ToList
      in
         thread
            %% Safe thread. Does not modify state
            FromList = OldPred.id|{@Replica getReverseKeys(OldPred.id $)}
            ToList   = NewPred.id|{@Replica getReverseKeys(NewPred.id $)}
            {DataLoop FromList ToList}
          end
      end

      proc {InsertData insertData(entries:Entries db:DB tag:trapp)}
         if Entries \= nil then
            {DBs.DB insert(Entries _/*Result*/)}
         end
      end

      %% --- Internal to the Pbeer ---
      proc {SetMsgLayer setMsgLayer(AMsgLayer)}
         MsgLayer := AMsgLayer
         NodeRef  := {@MsgLayer getRef($)}
      end

      proc {SetReplica setReplica(ReplicaMan)}
         Replica := ReplicaMan
      end

      %proc {SetTimeout setTimeout(ATime)}
      %   Timeout := ATime
      %end

      Events = events(
                     %% Trappist's API
                     becomeReader:  BecomeReader
                     executeTransaction:ExecuteTransaction
                     getLocks:      GetLocks
                     runTransaction:RunTransaction
                     %% Directly to Key/Value-Sets
                     add:           ToValueSet
                     remove:        ToValueSet
                     readSet:       ToValueSet
                     createSet:     ToValueSet
                     destroySet:    ToValueSet
                     %% For the TMs
                     ack:           ForwardToTM
                     initRTM:       InitRTM
                     registerRTM:   ForwardToTM
                     rtms:          ForwardToTM
                     setFinal:      ForwardToTM
                     vote:          ForwardToTM
                     voteAck:       ForwardToTM
                     %% For the TPs
                     brew:          Brew
                     final:         Final
                     %% Data management
                     insertData:    InsertData
                     newPred:       NewPred
                     %% Internal to the Pbeer
                     setMsgLayer:   SetMsgLayer
                     setReplica:    SetReplica
                     %setTimeout:    SetTimeout
                     %timeout:       TimeoutEvent
                     )

   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
%         Listener = FullComponent.listener
      end
      NodeRef  = {NewCell noref}
      MsgLayer = {NewCell Component.dummy}
      Replica  = {NewCell Component.dummy}

      TMs      = {Dictionary.new}
      TPs      = {Dictionary.new}

      local
         Args     = {Utils.addDefaults CallArgs def(maxKey:Constants.largeKey)}
      in
         DBMan    = Args.dbman
         MaxKey   = {NewCell Args.maxKey}
      end
      PairsDB  = {DBMan getCreate(name:trapp type:basic db:$)}
      SetsDB   = {DBMan getCreate(name:sets type:secrets db:$)}

      Self
   end

end  

