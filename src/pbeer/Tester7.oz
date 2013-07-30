
%% This file is meant to test the functionality of the functors implemented on
%% this module.

%% Solved a bug(Inject Partition after node failure), partition script 
%% Refreshing scheme of links among nodes
%% Churn Script(Churn parameter: % of node turnover per time unit per node)

%% New Modifications
%% Changed List operations


declare

SIZE = 12
TIMEUNIT = 5000 %%in milliseconds

{Property.put 'print.width' 1000}
{Property.put 'print.depth' 1000}

[BeerLogger Network PbeerMaker]={Module.link ["../logger/Logger.ozf" "../network/Network.ozf" "Pbeer.ozf"]}

[Viewer DrawGraph QTk]={Module.link ["../../pepino/LogViewer.ozf" "../../pepino/DrawGraph.ozf" "x-oz://system/wp/QTk.ozf"]}

LogFile="testlog"

ComLayer
MasterOfPuppets
MaxKey
Pbeers
AlivePbeers
NetRef
NetworkSize

TotalRefreshMessages
LoggerPort
proc{Logger M}
   case M
       of event(_ addLink(_) color:gray ...) then
          if @TotalRefreshMessages < @NetworkSize * @NetworkSize then
              {Port.send LoggerPort M}
              TotalRefreshMessages := @TotalRefreshMessages+1
          end
   else
         {Port.send LoggerPort M}
   end
end

CloseLogFile

thread
   O
   S={Port.new $ LoggerPort}
   MarkedNodes={NewCell nil}
   MarkedEdges={NewCell nil}
   FailedLinks={NewCell nil}
   PartitionedLinks={NewCell nil}

   proc{AddFailedLink S T}
      L E
   in
      L=@FailedLinks
      E=S#","#T
      if {List.member E @MarkedEdges} then
         {O removeEdge(S T black)}
      else
	 {O removeEdge(S T gray)}
         {O addEdge(S T red)}
      end
      FailedLinks:=E|L
   end

   proc{AddPartitionedLink S T}
      if {Not {List.member S#","#T @PartitionedLinks}} then
         L
         in
         L=@PartitionedLinks
         PartitionedLinks:=S#","#T|L
      end
   end

  proc{RemovePartitionedLink S T}
      L
   in
      L=@PartitionedLinks
      PartitionedLinks:={List.subtract L S#","#T}
  end

   proc{RemoveAllFailedLinks}
      case @FailedLinks
      of From#","#To|_ then
          {ForAll From#","#To|{List.subtract @FailedLinks From#","#To} 
                        proc{$ A}
                          case A of
                            F#","#T then
                              {O removeEdge(F T red)}
                          else
                              skip
                          end
                        end}
      [] nil then
         skip
       end
   end

   proc{RestoreFailedLink S T}
      E L
   in
      E = S#","#T
      L = @FailedLinks
      if {List.member E @MarkedEdges} then
         {O removeEdge(S T darkblue)}
      else
	 {O removeEdge(S T red)}
         {O addEdge(S T gray)}
      end
      FailedLinks:={List.subtract L E}
   end

   proc{Mark P}
      L
   in
      L = @MarkedNodes
      if {Not {List.member P L}} then
	 Info={O getNodeInfo({P getId($)} $)}
         in
	 {Info.canvas tk(itemconfigure Info.box width:3)}
	 MarkedNodes:=P|L
      end
   end

   proc{MarkEdge S T}
      L E
   in
      L=@MarkedEdges
      E = S#","#T
      if {Not {List.member E L}} then
         if {List.member E @FailedLinks} then
               {O removeEdge(S T red)}
               {O addEdge(S T darkblue)}
         else     
	       {O removeEdge(S T gray)}
               {O addEdge(S T black)}
         end
	 MarkedEdges:=E|L
      end
   end

   proc{UnMark P}
      L
   in
      L=@MarkedNodes
      if {List.member P L} then
	 Info={O getNodeInfo({P getId($)} $)}
         in
	 {Info.canvas tk(itemconfigure Info.box width:1)}
	 MarkedNodes:={List.subtract L P}
      end
   end

   proc{UnMarkEdge S T}
      L E
   in
      L=@MarkedEdges
      E=S#","#T
      if {List.member E L} then
         if {Not {List.member E @FailedLinks}} then
	      {O removeEdge(S T black)}
              {O addEdge(S T gray)}
         else
              {O removeEdge(S T darkblue)}
              {O addEdge(S T red)}
         end
	MarkedEdges:={List.subtract L E}
      end
   end

   proc{Do P Proc}
      {ForAll P|{List.subtract @MarkedNodes P} Proc}
   end

   proc{DoEdges P Q Proc}
      {ForAll P#","#Q|{List.subtract @MarkedEdges P#","#Q} Proc}
   end

   proc {CollectEdges Pbeer N}
      proc {CollectRound Start M First Last Round FNodes}
          if Round \= N then
                CurrentNodes = {LoopNetwork Start First M Last}
                FNodes := {List.append @FNodes CurrentNodes}
                RestNodes = {ListMinus @AlivePbeers @FNodes}%Remove only CurrentNodes 4 both-way failure
                in
                {ForAll CurrentNodes 
                       proc {$ Current}
	                  for P in RestNodes do
                              {MarkEdge Current.id {P getId($)}} 
                          end
                       end
                 }
                 {CollectRound @Last M First Last Round+1 FNodes}
          end
      end
      PbeersPerPartition = @NetworkSize div N
      LastNode = {NewCell nil}
      FromNodes = {NewCell nil}
      in
      {CollectRound {Pbeer getSucc($)} PbeersPerPartition Pbeer LastNode 1 FromNodes}
   end

   proc{DoPartition N}
      {CollectEdges @MasterOfPuppets N}
      case @MarkedEdges
      of From#","#To|_ then         
	{DoEdges From To proc{$ A}
                             if {Not {List.member A @FailedLinks}} then
                                   case A of
                                   F#","#T then
                                       {AddFailedLink F T}
                                       {AddPartitionedLink F T}
                                       {UnMarkEdge F T}
	                               FromPbeer = {GetPbeer F}
				       in  
				       {FromPbeer injectLinkFail(T)}
                                    else
                                       skip
                                    end
                               end
                       end}
      [] nil then
          skip
      end
      %{RemoveAllFailedLinks}
   end

   proc{DoPartitionRestore}
      case @PartitionedLinks
      of From#","#To|_ then
          {ForAll From#","#To|{List.subtract @PartitionedLinks From#","#To} 
                        proc{$ A}
                          case A of
                            F#","#T then
                              {RestoreFailedLink F T}
                              {UnMarkEdge F T}
                              {RemovePartitionedLink F T}
                              FromPbeer = {GetPbeer F}
		              in  
			      {FromPbeer restoreLink(T)}
                          else
                              skip
                          end
                        end}
      [] nil then
         skip
      end
   end

   proc{UINodeFailure P}
      Info={O getNodeInfo(P $)}
      in
      {Info.canvas tk(itemconfigure Info.box fill:red)}
      {O removeAllOutEdges(P)}
  end
   
in
   thread CloseLogFile={Viewer.writeLog S LogFile} end

   O={Viewer.interactiveLogViewer S}
   {O display(green:true red:true blue:true gray:true)}
   {O onParse(proc{$ E}
		 case E
		 of event(F succChanged(N M) color:green ...) then
		    {O removeEdge(F M green)}
                    {O removeEdge(F N gray)}         %For some reason if gray edge is there, 
                                                     %green edge remains invisible
		    {O addEdge(F N green)}
                 [] event(F predChanged(N M) color:darkblue ...) then
		    {O removeEdge(F M lightblue)}
                    {O removeEdge(F N gray)}        %For some reason if gray edge is there, 
                                                    %green edge remains invisible
		    {O addEdge(F N lightblue)}
		 [] event(F onRing(true) color:darkblue ...) then
		    Info={O getNodeInfo(F $)}
		 in
		    {Info.canvas tk(itemconfigure Info.box fill:yellow)}
		 [] event(F onRing(false) color:darkblue ...) then
		    Info={O getNodeInfo(F $)}
		 in
		    {Info.canvas tk(itemconfigure Info.box fill:white)}
		 [] event(F newSucc(N) ...) then
		    {O addEdge(F N green)}
	         [] event(F newPred(N) ...) then
		    {O addEdge(F N lightblue)}
		 [] event(F predNoMore(N) ...) then
		    {O removeEdge(N F green)}
                 [] event(F addLink(N) color:gray ...) then
                    if {Not {List.member N#","#F @MarkedEdges}} andthen
                          {Not {List.member N#","#F @FailedLinks}} then
                         {O removeEdge(N F gray)}
                         {O addEdge(N F gray)}
                    end
                 [] event(F crash(N) color:red ...) then
                    if {Not {List.member F#","#N @FailedLinks}} then
                        {AddFailedLink F N}
                        {UnMarkEdge F N}
                    end
                    if {Not {IsIdInList @AlivePbeers N}} then
                        {O removeEdge(F N red)}
                    end
                 [] event(F alive(N) color:green ...) then
                    if {List.member F#","#N @FailedLinks} then
                        {RestoreFailedLink F N} 
                        {UnMarkEdge F N}
                    end
                 [] event(_ failed(F) color:red ...) then
                    {UINodeFailure F}
		 else
		    skip
		 end	      
	      end)}
   {O onClick(proc{$ E}
		 proc{RunMenu L}
		    Menu={New Tk.menu tkInit(parent:E.canvas)}
		    {ForAll L
		     proc{$ E}
			case E of nil then
			   {New Tk.menuentry.separator tkInit(parent:Menu) _}
			[] T#P then
			   {New Tk.menuentry.command tkInit(parent:Menu
							    label:T
							    action:P) _}
			end
		     end}
		 in
		    {Menu tk(post {Tk.returnInt winfo(rootx E.canvas)}+E.x
			     {Tk.returnInt winfo(rooty E.canvas)}+E.y)}
		 end
	      in
		 case E of node(N ...) then
		    {ForAll @AlivePbeers
		     proc{$ P}
			if {P getId($)}==N then
			   {RunMenu ["Info..."#proc{$} {Browse {P getFullRef($)}} end
				     nil
				     "Mark"#proc{$} {Mark P} end
				     "UnMark"#proc{$} {UnMark P} end
				     nil
				     "Leave"#proc{$} 
                                                {Do P proc{$ P}
                                                      {UINodeFailure {P getId($)}}
                                                      {UnMark P}
			                              {FailANode P 1}
					         end} end
				     nil
				     "permFail"#proc{$}
						   {Do P proc{$ P}  
                                                       {UINodeFailure {P getId($)}}   
						       {UnMark P}
                                                       {FailANode P 0}
							 end}
						end
                                     "Congested"#proc{$} {Do P proc{$ P}
							          {P injectLinkDelay}
							        end} 
                                                  end]}
			end
		     end}
		 [] edge(From To ...) then
		    {RunMenu ["tempFail"#proc{$}
                                             {DoEdges From To proc{$ A}
                                                if {Not {List.member A @FailedLinks}} then
                                                     case A of
                                                     F#","#T then
                                                         {AddFailedLink F T}
                                                         {UnMarkEdge F T} 
						         FromPbeer = {GetPbeer F}
					                 in  
					                 {FromPbeer injectLinkFail(T)}
                                                     else
                                                         skip
                                                     end
                                                end
                                             end} 
					 end
			      "normal"#proc{$} {DoEdges From To proc{$ A}
                                                if {List.member A @FailedLinks} then
                                                     case A of
                                                     F#","#T then
                                                         {RestoreFailedLink F T}
							 {UnMarkEdge F T} 
						         FromPbeer = {GetPbeer F}
					                 in  
					                 {FromPbeer restoreLink(T)}
                                                     else
                                                         skip
                                                     end
                                                end
                                             end} end
                              "Mark"#proc{$} {MarkEdge From To} end
                              "UnMark"#proc{$} {UnMarkEdge From To} end]}
		 else skip end
	      end)}

		 
   {O onClose(proc{$ C}
		 {CloseLogFile}
		 {C tkClose}
	      end)}

   {O onEnter(proc{$ E}
                 F P N
                 in
                 {String.token E &: F P}
                 case F
		 of "Partition" then
                    N = {String.toInt P}
                    if N=<@NetworkSize then
                        {DoPartition N}
                    else
                        skip
                    end
                 [] "GlobalDelay" then
                    {ForAll @AlivePbeers
		     proc{$ P}
			{P injectLinkDelay}
		     end}
                 [] "Join" then
		     {InjectJoin}
                 [] "RefreshLinks" then
                     TotalRefreshMessages := 0
                 [] "Churn" then
                     N = {String.toInt P}
                     if N=<100 andthen N>=0 then
                         {InjectChurn N}
                     else
                         skip
                     end
                 [] "RestorePartition" then
                     {DoPartitionRestore}
                 else
                    skip
                 end
               end)}
end

proc{FailANode Pbeer IsGentle}
  if {@MasterOfPuppets getId($)}=={Pbeer getId($)} then
    CandidatePuppet 
    in
    CandidatePuppet = {@MasterOfPuppets getSucc($)}
    MasterOfPuppets := {GetPbeer CandidatePuppet.id}
    NetRef := {@MasterOfPuppets getFullRef($)}
  end 
  AlivePbeers:={ListIdRemove @AlivePbeers {Pbeer getId($)}}
  if IsGentle == 1 then
     {Logger comment(leave({Pbeer getId($)}) color:red)}
     {Pbeer leave}
  else
     {Logger comment(permFail({Pbeer getId($)}) color:red)}
     {Pbeer injectPermFail}
  end
  NetworkSize := @NetworkSize - 1
end

proc {InjectJoin}
  Pbeer
  in
  Pbeer = {NewPbeer}
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
           {Logger event({Pbeer getId($)} failed({Pbeer getId($)}) color:red)}
           {FailANode Pbeer 0}
        else
           {InjectFailure MorePbeers}
        end
   [] nil then
	skip
   end
end

proc {InjectChurn ChurnParam}
  EventsPerTU = (ChurnParam*2*@NetworkSize) div 100
  proc {ChurnRoulette Period}
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
     {ChurnRoulette Period}
  end
  DelayPeriod = TIMEUNIT div EventsPerTU
  in
  {ChurnRoulette DelayPeriod}
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

%% Return a list with elements of L1 that are not present in L2
fun {ListMinus L1 L2}
   case L1#L2
   of (H1|T1)#(H2|T2) then
      {ListMinus {ListIdRemove L1 H2.id} T2}
   [] nil#_ then
      nil
   [] _#nil then
      L1
   end
end

fun {IsIdInList L PbeerId}
   case L
   of (H|T) then
      if {H getId($)} == PbeerId then
         true
      else
         {IsIdInList T PbeerId}
      end
   [] nil then
      false
   end
end

fun {LoopNetwork Pbeer Master Size Last}
      proc {Loop Current First Counter Result Last}
         if Current \= nil andthen
	    Current.id \= {First getId($)} andthen
	    Counter =< Size then
            Succ
            in
            Result := Current|@Result
            Succ = {ComLayer sendTo(Current getSucc($))}
            Last := Succ
	    {Loop Succ First Counter+1 Result Last}
         end
      end
      Result
   in
      Result = {NewCell nil}
      {Loop Pbeer Master 1 Result Last}
      @Result
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
%%--------------- Creating the network -------------------
   proc {TestCreate}
      proc {CreateAPbeer N}
         if N > 0 then
             Pbeer
             in
             Pbeer = {NewPbeer}
	     {Pbeer setLogger(Logger)}
             {Pbeer join(@NetRef)}
             Pbeers :=  Pbeer|@Pbeers
             AlivePbeers := Pbeer|@AlivePbeers   
             {CreateAPbeer N-1}
         end
      end
      in 
      MasterOfPuppets := {PbeerMaker.new args}
      MaxKey = {NewCell {@MasterOfPuppets getId($)}}
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
   TotalRefreshMessages = {NewCell 0}
   NetworkSize = {NewCell SIZE}
   Pbeers = {NewCell nil}
   AlivePbeers = {NewCell nil}
   {TestCreate}	

