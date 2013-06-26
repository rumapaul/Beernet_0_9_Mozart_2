/*-------------------------------------------------------------------------
 *
 * PbeerIdList.oz
 *
 *    This files contains general functions asociated with list of PBeer Ids. 
 *    Lists are sorted.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Ruma Paul <ruma.paul@uclouvain.be>
 *
 *    Last change: $Revision: 1 $ $Author: ruma $
 *
 *    $Date: 2013-03-25 14:30:21 +0200 (Mon, 25 March 2013) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   AddId
   RemoveId
   IsIdIn
   New

define

   fun {AddId PeerId L}
      case L
      of H|T then
         if H < PeerId then
            H|{AddId PeerId T}
         elseif H == PeerId then
            L
         else
            PeerId|L
         end
      [] nil then
         PeerId|nil
      end
   end

   %% Return true if PeerId is found in list L
   %% Precondition: L is sorted
   fun {IsIdIn PeerId L}
      case L
      of H|T then
         if H == PeerId then
            true
         elseif H < PeerId then
            {IsIdIn PeerId T}
         else
            false
         end
      [] nil then
         false
      end
   end

   %% Remove a Peer from a List
   fun {RemoveId PeerId L}
      case L
      of H|T then
         if H == PeerId then
            T
         else
            H|{RemoveId PeerId T}
         end
      [] nil then
         nil
      end
   end

   %% For the sake of completeness of the ADT
   fun {New}
      nil
   end
end
