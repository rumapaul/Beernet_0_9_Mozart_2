
%% This file is meant to test the functionality of the functors implemented on
%% this module.

declare

SIZE=6

{Property.put 'print.width' 1000}
{Property.put 'print.depth' 1000}

[BeerLogger Network PbeerMaker]={Module.link ["../logger/Logger.ozf" "../network/Network.ozf" "Pbeer.ozf"]}

[Viewer DrawGraph QTk]={Module.link ["../../pepino/LogViewer.ozf" "../../pepino/DrawGraph.ozf" "x-oz://system/wp/QTk.ozf"]}

LogFile="testlog"

ComLayer
Log
MasterOfPuppets
MaxKey
Pbeers
NetRef
PbeersAfterMassacre

TotalLogMessages
LoggerPort
proc{Logger M}
   TotalLogMessages := @TotalLogMessages + 1
   if @TotalLogMessages < 100 then
       {Port.send LoggerPort M}
   else
       case M
       of event(F addLink(N) color:gray ...) then
          skip
       else 
          {Port.send LoggerPort M}
       end
   end
end

CloseLogFile

thread
   S={Port.new $ LoggerPort}
   O
   MarkedNodes={NewCell nil}
   MarkedEdges={NewCell nil}
   FailedLinks={NewCell nil}

   proc{AddFailedLink S T}
      OM NM
   in
      OM=FailedLinks:=NM
      if {List.member S#","#T @MarkedEdges} then
         {O removeEdge(S T black)}
      else
	 {O removeEdge(S T gray)}
         {O addEdge(S T red)}
      end
      NM=S#","#T|OM
   end

   proc{RestoreFailedLink S T}
      OM NM
   in
      OM=FailedLinks:=NM
      if {List.member S#","#T @MarkedEdges} then
         {O removeEdge(S T darkblue)}
      else
	 {O removeEdge(S T red)}
         {O addEdge(S T gray)}
      end
      NM={List.subtract OM S#","#T}
   end

   proc{Mark P}
      OM NM
   in
      OM=MarkedNodes:=NM
      if {Not {List.member P OM}} then
	 Info={O getNodeInfo({P getId($)} $)}
      in
	 {Info.canvas tk(itemconfigure Info.box width:3)}
	 NM=P|OM
      else
	 NM=OM
      end
   end

   proc{MarkEdge S T}
      OM NM
   in
      %{System.showInfo "in MarkEdge"}
      OM=MarkedEdges:=NM
      if {Not {List.member S#","#T OM}} then
         if {List.member S#","#T @FailedLinks} then
               {O removeEdge(S T red)}
               {O addEdge(S T darkblue)}
         else     
	       {O removeEdge(S T gray)}
               {O addEdge(S T black)}
         end
	 NM=S#","#T|OM
      else
	 NM=OM
      end
   end

   proc{UnMark P}
      OM NM
   in
      OM=MarkedNodes:=NM
      if {List.member P OM} then
	 Info={O getNodeInfo({P getId($)} $)}
      in
	 {Info.canvas tk(itemconfigure Info.box width:1)}
	 NM={List.subtract OM P}
      else
	 NM=OM
      end
   end

   proc{UnMarkEdge S T}
      OM NM
   in
      OM=MarkedEdges:=NM
      if {List.member S#","#T OM} then
         if {Not {List.member S#","#T @FailedLinks}} then
	      {O removeEdge(S T black)}
              {O addEdge(S T gray)}
         else
              {O removeEdge(S T darkblue)}
              {O addEdge(S T red)}
         end
	 NM={List.subtract OM S#","#T}
      else
	 NM=OM
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
                RestNodes = {ListMinus Pbeers @FNodes} %Remove only CurrentNodes for both-way failure 
                in
                {ForAll CurrentNodes 
                       proc {$ Current}
	                  for P in RestNodes do
                              {MarkEdge Current.id {P getId($)}} 
                          end
                          {MarkEdge Current.id {First getId($)}}
                       end
                 }
                 {CollectRound @Last M First Last Round+1 FNodes}
          end
      end
      PbeersPerPartition = SIZE div N
      LastNode = {NewCell nil}
      FromNodes = {NewCell nil}
      in
      {CollectRound {Pbeer getSucc($)} PbeersPerPartition Pbeer LastNode 1 FromNodes}
   end

   proc{DoPartition N}
      {CollectEdges MasterOfPuppets N}
      case @MarkedEdges
      of From#","#To|T then
          {DoEdges From To proc{$ A}
                             if {Not {List.member A @FailedLinks}} then
                                   case A of
                                   F#","#T then
                                       {AddFailedLink F T} 
	                               FromPbeer = {GetPbeer F}
				       in
                                       {UnMarkEdge F T}  
				       {FromPbeer injectLinkFail(T)}
                                    else
                                       skip
                                    end
                               end
                       end}
      [] nil then
          skip
      end
   end
   
in
   thread CloseLogFile={Viewer.writeLog S LogFile} end

   O={Viewer.interactiveLogViewer S}
   {O display(green:true red:true blue:true gray:true)}
   {O onParse(proc{$ E}
		 case E
		 of event(F succChanged(N M) color:green ...) then
                    %{System.showInfo F#" "#N#" "#M}
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
                 [] event(F alive(N) color:green ...) then
                    if {List.member F#","#N @FailedLinks} then
                        {RestoreFailedLink F N} 
                        {UnMarkEdge F N}
                    end
		    %in
		    %{Info.canvas tk(itemconfigure Info.box fill:red)}
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
		    {ForAll Pbeers
		     proc{$ P}
			if {P getId($)}==N then
			   {RunMenu ["Info..."#proc{$} {Browse {P getFullRef($)}} end
				     nil
				     "Mark"#proc{$} {Mark P} end
				     "UnMark"#proc{$} {UnMark P} end
				     nil
				     "Leave"#proc{$} {Do P proc{$ P}
							      {P leave}
							      {Logger comment(leave({P getId($)}) color:red)}
							   end} end
				     nil
				     "permFail"#proc{$}
						   {Do P proc{$ P}
							    {O removeAllOutEdges({P getId($)})}
							    Info={O getNodeInfo({P getId($)} $)}
							 in
							    {Info.canvas tk(itemconfigure Info.box fill:red)}
							    {Logger comment(permFail({P getId($)}) color:red)}
							    {UnMark P}
							    {P injectPermFail}
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
						         FromPbeer = {GetPbeer F}
					                 in
                                                         {UnMarkEdge F T}  
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
						         FromPbeer = {GetPbeer F}
					                 in
                                                         {UnMarkEdge F T}  
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
                    if N=<SIZE then
                        {DoPartition N}
                    else
                        skip
                    end
                 [] "GlobalDelay" then
                    {ForAll Pbeers
		     proc{$ P}
			{P injectLinkDelay}
		     end}
                 else
                    skip
                 end
               end)}


%   {O onRightClick(...)}
%   {O runToEnd}
   
%   {Viewer.drawLog S c(green:false red:false blue:false gray:true)} %{Port.new $ LoggerPort}}

   %% green are session handshaking messages
   %% red are sideband direct messages
   %% blue are session membership events
   %% gray are session messages, turn it on to see T1 protocol messages
   %% darkblue are the messages & events provided by T1
   
end

fun {ListRemove L1 P}
  case L1
   of (H1|T1) then
      if {H1 getId($)} == P.id then
         T1
      else
         H1|{ListRemove T1 P}
      end
   [] nil then
      nil
   end
end

%% Return a list with elements of L1 that are not present in L2
fun {ListMinus L1 L2}
   case L1#L2
   of (H1|T1)#(H2|T2) then
      {ListMinus {ListRemove L1 H2} T2}
   [] nil#_ then
      nil
   [] _#nil then
      L1
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
	for P in Pbeers do
               if {P getId($)}==PbeerId then
		    Pbeer = P
	       end
	end
	Pbeer
end
%%--------------- Creating the network -------------------
   proc {TestCreate}
      MasterOfPuppets = {PbeerMaker.new args}
      MaxKey = {NewCell {MasterOfPuppets getId($)}}
      Log = {BeerLogger.new 'lucifer.log'}
      %{MasterOfPuppets setLogger(Log.logger)}
      {MasterOfPuppets setLogger(Logger)}
      Pbeers = {List.make SIZE-1}
      NetRef = {MasterOfPuppets getFullRef($)}
      for Pbeer in Pbeers do
         Pbeer = {NewPbeer}
         %{Pbeer setLogger(Log.logger)}
	 {Pbeer setLogger(Logger)}
         {Pbeer join(NetRef)}
         %{Delay 100}
      end

      %Pbeers = {List.append TmpPbeers MasterOfPuppets} 
      ComLayer = {Network.new}
   end
in
   TotalLogMessages = {NewCell nil}
   TotalLogMessages := 0
   {TestCreate}	

