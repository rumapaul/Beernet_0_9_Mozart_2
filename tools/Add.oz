/*-------------------------------------------------------------------------
 *
 * Add.oz
 *
 *    pbeer subcommand. It connect to any peer and triggers an add operation.
 *    The value is added to the majority of the replicas hosting the set.
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
 *-------------------------------------------------------------------------
 */

functor
import
   Application
   System
   PbeerBaseArgs  at 'PbeerBaseArgs.ozf'
   SetsCommon     at 'SetsCommon.ozf'
export
   DefArgs
   Run
define
   DefArgs = nil

   proc {Run Args}
      Pbeer
      Key
      MyPort
      Outcome
   in
      if Args.help then
         {PbeerBaseArgs.helpMessage [key value cap ring store] nil add}
         {Application.exit 0}
      end
      Pbeer = {SetsCommon.getPbeer Args.store Args.ring}
      MyPort= {Port.new Outcome}
      Key   = {SetsCommon.capOrKey Args.cap Args.key}
      {Pbeer add(Key Args.value MyPort)}
      {System.showInfo Outcome.1}
      {Application.exit 0}
   end
end
