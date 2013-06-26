%% This file is meant to test the functionality of the functors implemented on
%% this module.

functor

import
   Application
   OS
   Property
   System
   Logger         at '../logger/Logger.ozf'
   Network        at '../network/Network.ozf'
   PbeerMaker     at 'Pbeer.ozf'

define
   SIZE  = 42
   CHURN = 7
   FAILURE = 7

   ComLayer
   Log
   MasterOfPuppets
   MaxKey
   Pbeers
   PbeersAfterMassacre
   PbeersAfterChurn
   TestBuild
   TestBuildPred
   TestMassacre
   TestMassacrePred
   TestAfterChurn
   TestChurnPred
   NetRef

   fun {GetMaxId L}
	TmpId
        in	
	case L
            of Pbeer|MorePbeers then
		{Pbeer getId(TmpId)}
		%{System.printInfo TmpId#","}
		if TmpId > @MaxKey then
	        	MaxKey := TmpId
 		end
                Pbeer|{GetMaxId MorePbeers}
	[] nil then
		nil
	end
   end		

   fun {Kill N L}
      fun {RussianRoulette I L FinalCount}
         if I < N then
            case L
            of Pbeer|MorePbeers then
               if {OS.rand} mod 7 < 1 then
		  local
			TmpId
                  in
                        {Pbeer getId(TmpId)}
			if TmpId == @MaxKey then
				{System.showInfo "Killing Peer with MaxKey"}
		  	end
		  end	
		  
		  {Pbeer injectPermFail}
                  {RussianRoulette I+1 MorePbeers FinalCount}
               else
                  Pbeer|{RussianRoulette I MorePbeers FinalCount}
               end
            [] nil then
               FinalCount = I
               nil
            end
         else
            FinalCount = I
            L
         end
      end
      fun {KillingLoop I L}
         if I < N then
            NewI NewL
         in
            NewL = {RussianRoulette I L NewI}
            {KillingLoop NewI NewL}
         else
            L
         end
      end
   in
      {KillingLoop 0 L}
   end

   fun {Partition Pbeer}
      fun {FirstHalf I Current}
         if I < 5 then
	    Succ
            in
            %Succ = {ComLayer sendTo(Current getSucc($))}
	    {Current getSucc(Succ)}  
	    {Current injectPermFail}
            {FirstHalf I+1 Succ}
         else
            I
         end
      end
   in
      {FirstHalf 0 {Pbeer getSucc($)}}
   end

   fun {Churn N L}
      fun {ChurnRoulette I J L FinalI FinalJ}
         if I < N orelse J < N then
            case L
            of Pbeer|MorePbeers then
               Luck = {OS.rand} mod 7 
            in
               if Luck < 1 andthen I < N then	
                  {Pbeer injectPermFail}
		  {Delay 4000}
                  {ChurnRoulette I+1 J MorePbeers FinalI FinalJ}
               elseif Luck > 5 andthen J < N then
                  New
		  NetRef	
               in
		  NetRef = {MasterOfPuppets getFullRef($)}
                  New = {NewPbeer}
		  {New join(NetRef)}	
		  {Delay 1000}
                  Pbeer|New|{ChurnRoulette I J+1 MorePbeers FinalI FinalJ}
               else
                  Pbeer|{ChurnRoulette I J MorePbeers FinalI FinalJ}
               end
            [] nil then
               FinalI = I
               FinalJ = J
               nil
            end
         else
            FinalI = I
            FinalJ = J
            L
         end
      end
      fun {ChurnLoop I J L}
         if I < N orelse J < N then
            NewI NewJ NewL
         in
            NewL = {ChurnRoulette I J L NewI NewJ}
            {ChurnLoop NewI NewJ NewL}
         else
            L
         end
      end
   in
      {ChurnLoop 0 0 L}
   end

   fun {BoolToString B}
      if B then "PASSED"
      else "FAILED" end
   end

   fun {LoopNetwork Pbeer Size}
      fun {Loop Current Pred First Counter OK Error}
         if Current == nil then
            result(passed:false
                   error:Error#" - Wrong Size "#Counter#" != "#Size)
         elseif Current.id == First.id then
            {System.showInfo Current.id}
            if Counter == Size then
               if OK then
                  result(passed:true)
               else
                  result(passed:OK error:Error)
               end
            else
               result(passed:false
                      error:Error#" - Wrong Size "#Counter#" != "#Size)
            end
         else
            Succ
         in
            Succ = {ComLayer sendTo(Current getSucc($))}
            {System.printInfo Current.id#"->"}
            if Succ.id < Current.id andthen Current.id \= @MaxKey then
               {Loop Succ Current First Counter+1
                     false Error#Current.id#"->"#Succ.id#" "}
            else
               {Loop Succ Current First Counter+1 OK Error}
            end
         end
      end
      First
      Result
   in
      First = {Pbeer getFullRef($)}
      {System.showInfo "Network "#First.ring.name}
      {System.printInfo First.pbeer.id#"->"}
      Result = {Loop {Pbeer getSucc($)} 
                     First.pbeer 
                     First.pbeer 
                     1 
                     true
                     nil}
      if Result.passed then
         {System.showInfo "\n+++ PASSED +++"}
      else
         {System.showInfo "\n+++ FAILED +++"}
         {System.showInfo "Error: "#Result.error}
      end
      Result.passed
   end

   fun {LoopNetworkPred Pbeer Size}
      fun {Loop Current Succ First Counter OK Error}
         if Current == nil then
            result(passed:false
                   error:Error#" - Wrong Size "#Counter#" != "#Size)
         elseif Current.id == First.id then
            {System.showInfo Current.id}
            if Counter == Size then
               if OK then
                  result(passed:true)
               else
                  result(passed:OK error:Error)
               end
            else
               result(passed:false
                      error:Error#" - Wrong Size "#Counter#" != "#Size)
            end
         else
            Pred
            in
            Pred = {ComLayer sendTo(Current getPred($))}
            {System.printInfo Current.id#"->"}
	    if Pred == nil then
		result(passed:false
                   error:Error#" - Pred is Nil")
	    elseif Pred.id > Current.id andthen Pred.id \= @MaxKey then
      		{Loop Pred Current First Counter+1
                 	    false Error#Current.id#"->"#Pred.id#" "}
            else
               	{Loop Pred Current First Counter+1 OK Error}
            end
         end
      end
      First
      Result
   in
      First = {Pbeer getFullRef($)}
      {System.showInfo "Network following PRED "#First.ring.name}
      {System.printInfo First.pbeer.id#"->"}
      Result = {Loop {Pbeer getPred($)} 
                     First.pbeer 
                     First.pbeer 
                     1 
                     true
                     nil}
      if Result.passed then
         {System.showInfo "\n+++ PASSED +++"}
      else
         {System.showInfo "\n+++ FAILED +++"}
         {System.showInfo "Error: "#Result.error}
      end
      Result.passed
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

   %%--------------- The tests ------------------------------

   %%--------------- Creating the network -------------------
   proc {TestCreate}
      MasterOfPuppets = {PbeerMaker.new args}
      MaxKey = {NewCell {MasterOfPuppets getId($)}}
      Log = {Logger.new 'lucifer.log'}
      {MasterOfPuppets setLogger(Log.logger)}
      Pbeers = {List.make SIZE-1}
      NetRef = {MasterOfPuppets getFullRef($)}
      for Pbeer in Pbeers do
         Pbeer = {NewPbeer}
         {Pbeer setLogger(Log.logger)}
         {Pbeer join(NetRef)}
         %{Delay 100}
      end
      ComLayer = {Network.new}
      {Delay 1000}
      local
         P I S
      in
         {MasterOfPuppets getPred(P)}
         {MasterOfPuppets getId(I)}
         {MasterOfPuppets getSucc(S)}
         {System.showInfo "MASTER: "#P.id#"<-"#I#"->"#S.id}
      end
      TestBuild = {LoopNetwork MasterOfPuppets SIZE}
      TestBuildPred = {LoopNetworkPred MasterOfPuppets SIZE}
   end

   %%--------------- Introducing Churn -----------------------
   proc {TestChurn}
      {System.showInfo "Killing "#FAILURE#" Pbeers"}
      PbeersAfterMassacre = {Kill FAILURE Pbeers}
      {Delay 4000}
      
      {System.showInfo "MaxKey:"#@MaxKey}
      MaxKey := {MasterOfPuppets getId($)}
      PbeersAfterMassacre = {GetMaxId PbeersAfterMassacre}
      {System.showInfo "New MaxKey:"#@MaxKey}
      
      TestMassacre = {LoopNetwork MasterOfPuppets {Length PbeersAfterMassacre}+1}
      %TestMassacrePred = {LoopNetworkPred MasterOfPuppets {Length PbeersAfterMassacre}+1}	
      
      {System.showInfo "CHURN of "#CHURN#" Pbeers"}
      PbeersAfterChurn = {Churn CHURN PbeersAfterMassacre}
      {Delay 4000}

      {System.showInfo "MaxKey:"#@MaxKey}
      MaxKey := {MasterOfPuppets getId($)}
      PbeersAfterChurn = {GetMaxId PbeersAfterChurn}
      {System.showInfo "New MaxKey:"#@MaxKey}

      TestAfterChurn = {LoopNetwork MasterOfPuppets {Length PbeersAfterChurn}+1}
      %TestChurnPred = {LoopNetworkPred MasterOfPuppets {Length PbeersAfterChurn}+1}	
   end

   proc {TestPartition}
      local
         Pred NumberOfFailure
      in
         {MasterOfPuppets getPred(Pred)}
	 NumberOfFailure = {Partition MasterOfPuppets}
      	 {Delay 4000}
	 TestAfterChurn = {LoopNetwork Pred SIZE-NumberOfFailure}
      end
   end

   SumText = tests(
                  build:      "Build Test: "
                  buildPred:  "Build Test Pred: "
                  churn:      "Churn Test: "
                  churnPred:  "Churn Test Pred: "
                  massacre:   "Failure Test: "
                  massaPred:  "Failure Test Pred: "
                  rsend:      "Reliable Send Test: "
                  )

   SumVariables = tests(
                        build:      TestBuild
                        buildPred:  TestBuildPred
                        churn:      TestAfterChurn
                        churnPred:  TestChurnPred
                        massacre:   TestMassacre
                        massaPred:  TestMassacrePred
                        %rsend:      TestRSend
                        )

   proc {TestSummary Tests}
      proc {SummaryLoop Tests}
         case Tests
         of Test|MoreTests then
            {Say SumText.Test#{BoolToString SumVariables.Test}}
            {SummaryLoop MoreTests}
         [] nil then
            skip
         end
      end
   in
      {Say "*** Test Summary ***"}
      if Tests == [all] then
         {SummaryLoop [build buildPred massacre churn]}
      else
         {SummaryLoop Tests}
      end
   end

   %% For feedback
   Show   = System.show
   Say    = System.showInfo
   Args
   
   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" <test> [option]"}
      {Say ""}
      {Say "Tests:"}
      {Say "\tall\tRun all tests"}
      {Say "\tcreate\tBootstrap a relaxed ring"}
      {Say "\tchurn\tTest the relaxed-ring with some churn"}
      {Say ""}
      {Say "Options:"}
      {Say "  -h, -?, --help\tThis help"}
   end

in
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
   of Command|nil then
      case Command
      of "all" then
         {TestCreate}
         {TestChurn}
         {TestSummary [all]}
      [] "create" then
         {TestCreate}
         {TestSummary [build buildPred]}
      [] "churn" then
         {TestCreate}
         {TestChurn}
         {TestSummary [massacre churn]}
      [] "partition" then
	 {TestCreate}
	 {TestPartition}
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
