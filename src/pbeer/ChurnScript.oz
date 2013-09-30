%% This file is the automated script for Churn
%% Required Parameters: 
%%	Beernet Network Size
%%      Churn value(Percentage of node turn-over per time unit per node)

functor

import
   Application
   OS
   Property
   System
   BeerLogger         at '../logger/Logger.ozf'
   Network        at '../network/Network.ozf'
   PbeerMaker     at 'Pbeer.ozf'

define
   TIMEUNIT = 5000 %%in milliseconds
   TESTDURATION = 60000 %%in milliseconds

   ComLayer
   MasterOfPuppets
   Pbeers
   AlivePbeers
   NetRef
   NetworkSize
   %Log
   MaxKey
   TotalMessages

   Churn
   EventsPerTU

   Say    = System.showInfo
   Args
   
   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   proc{Logger M}
      case M
         of out(src:_ n:_ dest:_ msg:_ tag:_ ...) then
             TotalMessages := @TotalMessages+1
      else
          skip
      end
   end

   fun {NewPbeer}
      New TmpId
   in
      New = {PbeerMaker.new args}
      {New getId(TmpId)}
      if TmpId > @MaxKey then
         MaxKey := TmpId
      end
      New
   end

   fun {GetPbeer PbeerId}
	Pbeer
   in
	for P in @AlivePbeers do
               if {P getId($)}==PbeerId then
		    Pbeer = P
	       end
	end
	Pbeer
   end


   fun {ListIdRemove L PId}
      case L
        of (H|T) then
           if {H getId($)} == PId then
              T
           else
              H|{ListIdRemove T PId}
           end
        [] nil then
           nil
      end
   end

   proc {TestBranches Pbeer}
      proc {Loop Current Prev First Counter Result}
         if Current \= nil then
            Succ Pred
            in
            Pred = {ComLayer sendTo(Current getPred($))}
            Succ = {ComLayer sendTo(Current getSucc($))}
            if Pred.id \= Prev then
               Result := @Result +1
            end
            if Current.id \= First then
	        {Loop Succ Current.id First Counter+1 Result}
            else
                {System.showInfo "Total Number of Nodes on branch:"#@NetworkSize-Counter}
                {System.showInfo "Total branches:"#@Result}
            end
         end
      end
      Result 
   in
      Result = {NewCell 0}
      {Loop {Pbeer getSucc($)} {Pbeer getId($)} {Pbeer getId($)} 1 Result} 
   end


   proc {FailANode Pbeer IsGentle}
      if {@MasterOfPuppets getId($)}=={Pbeer getId($)} then
         CandidatePuppet 
         in
         CandidatePuppet = {@MasterOfPuppets getSucc($)}
         MasterOfPuppets := {GetPbeer CandidatePuppet.id}
         NetRef := {@MasterOfPuppets getFullRef($)}
      end 
      AlivePbeers:={ListIdRemove @AlivePbeers {Pbeer getId($)}}
      if IsGentle == 1 then
        {Pbeer leave}
      else
        {Pbeer injectPermFail}
      end
      NetworkSize := @NetworkSize - 1
   end

   proc {InjectJoin}
     Pbeer
     in
     Pbeer = {NewPbeer}
     %{Pbeer setLogger(Log.logger)}
     {Pbeer setLogger(Logger)}
     {Pbeer join(@NetRef)}
     Pbeers :=  Pbeer|@Pbeers
     AlivePbeers := Pbeer|@AlivePbeers
     NetworkSize := @NetworkSize + 1
  end

  proc {InjectFailure L}
     case L
        of Pbeer|MorePbeers then
           FailureLuck = {OS.rand} mod 2 
           in
           if FailureLuck == 0 then	
              {FailANode Pbeer 0}
           else
              {InjectFailure MorePbeers}
           end
        [] nil then
	   skip
     end
   end

   proc {InjectChurn}
      TotalEvents = (TESTDURATION div TIMEUNIT) * EventsPerTU
      proc {ChurnRoulette Period NumEvents} 
         Luck = {OS.rand} mod 8 
         in
         if Luck<4 then
            {System.showInfo "Failure"}
            {InjectFailure @AlivePbeers}
         else
            {System.showInfo "Join"}
	    {InjectJoin}
         end  
         {Delay Period}
         if NumEvents < TotalEvents then
            {ChurnRoulette Period NumEvents+1}
         else
            skip
         end
      end
      DelayPeriod = TIMEUNIT div EventsPerTU
      in
      {ChurnRoulette DelayPeriod 1}
   end

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" <parameters> [option]"}
      {Say ""}
      {Say "Parameters:"}
      {Say "\tSize of Network"}
      {Say "\tChurn Value(Percentage of turn-over per time unit per node)"}
      {Say ""}
      {Say "Options:"}
      {Say "  -h, -?, --help\tThis help"}
   end

   proc {TestCreate}
      proc {CreateAPbeer N}
         if N > 0 then
             Pbeer
             in
             Pbeer = {NewPbeer}
	     %{Pbeer setLogger(Log.logger)}
             {Pbeer setLogger(Logger)}
             {Pbeer join(@NetRef)}
             Pbeers :=  Pbeer|@Pbeers
             AlivePbeers := Pbeer|@AlivePbeers   
             {CreateAPbeer N-1}
         end
      end
      in
      %Log = {Logger.new 'lucifer.log'} 
      MasterOfPuppets := {PbeerMaker.new args}
      MaxKey = {NewCell {@MasterOfPuppets getId($)}}
      %{@MasterOfPuppets setLogger(Log.logger)}
      {@MasterOfPuppets setLogger(Logger)}
      Pbeers :=  @MasterOfPuppets|@Pbeers
      AlivePbeers := @MasterOfPuppets|@AlivePbeers
      NetRef := {@MasterOfPuppets getFullRef($)}
      {CreateAPbeer @NetworkSize-1}
      ComLayer = {Network.new}
   end

in
   MasterOfPuppets = {NewCell nil}
   NetRef = {NewCell nil}
   NetworkSize = {NewCell nil}
   Pbeers = {NewCell nil}
   AlivePbeers = {NewCell nil}
   TotalMessages = {NewCell 0}
   
   %% Defining input arguments
   Args = try
             {Application.getArgs
              record(
                     help(single char:[&? &h] default:false)
                     )}

          catch _ then
             {Say 'Unrecognised arguments'}
             optRec(help:true)
          end

   %% Help message
   if Args.help then
      {HelpMessage}
      {Application.exit 0}
   end
   
   case Args.1
   of S|C|nil then
      NetworkSize := {String.toInt S}
      Churn = {String.toInt C}
      EventsPerTU = (Churn*2*@NetworkSize) div 100
      if Churn=<100 andthen EventsPerTU > 0 then
           {Say "Goiing to run the test"}
           {TestCreate}         
           {InjectChurn}
           {System.showInfo "Total Messages:"#@TotalMessages}
           {TestBranches @MasterOfPuppets}
      else 
           {Say "ERROR: Invalid invocation\n"}
           {HelpMessage}
      end       
   else
      {Say "ERROR: Invalid invocation\n"}
      {HelpMessage}
   end

   {Application.exit 0}
end
