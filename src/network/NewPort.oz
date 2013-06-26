/*-------------------------------------------------------------------------
 *
 * NewPort.oz
 *
 *    Implements simulated distributed port to inject link failure
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: ruma $
 *
 *    $Date: 2013-03-06 16:54:21 +0200 (Wed, 6 March 2013) $
 *
 * NOTES
 *      
 *    This is an implementation of module 2.3 of R. Guerraouis book on reliable
 *    distributed programming. Properties "reliable delivery", "no duplication"
 *    and "no creation" are guaranteed by the implementation of Port in Mozart.
 *
 * EVENTS
 *
 *    Accepts: send(Dest Msg) - Sends message Msg to destination Dest. Dest
 *    must be an Oz Port
 *
 *    Indication: pp2pDeliver(Src Msg) - Delivers message Msg sent by source
 *    Src.
 *    
 *-------------------------------------------------------------------------
 */

functor

import
   Component   at '../corecomp/Component.ozf'

export
   New

define

fun {New}
      SitePort       % Port to receive messages
      Listener       % Upper layer component
      FullComponent  % This component

      proc {GetPort getPort(P)}
         P = SitePort
      end

