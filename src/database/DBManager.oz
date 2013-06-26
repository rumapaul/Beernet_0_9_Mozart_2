/*-------------------------------------------------------------------------
 *
 * DBManager.oz
 *
 *    This components creates and provides access to different instances of
 *    simple databases.
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
   Component   at '../corecomp/Component.ozf'
   SimpleSDB   at '../database/SimpleSDB.ozf'
   SimpleDB    at '../database/SimpleDB.ozf'
export
   New
define

   fun {New}
      Self
      %Listener
      DBs
      DBMakers = dbs(basic:SimpleDB secrets:SimpleSDB)

      proc {Create create(name:DBid type:Type db:?NewDB)}
         NODB
      in
         NODB = {Name.new}
         if NODB == {Dictionary.condGet DBs DBid NODB} then
            NewDB = {DBMakers.Type.new}
            DBs.DBid := NewDB
         else
            NewDB = error(name_in_use)
         end
      end

      proc {Get get(name:DBid db:?TheDB)}
         TheDB = {Dictionary.condGet DBs DBid error(no_db)}
      end

      proc {GetCreate getCreate(name:DBid type:Type db:?NewDB)}
         NewDB = {Dictionary.condGet DBs DBid {DBMakers.Type.new}}
         DBs.DBid := NewDB
      end

      Events = events(
                     %% Key/Value pairs
                     create:     Create
                     get:        Get
                     getCreate:  GetCreate
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
      end

      DBs      = {Dictionary.new}
      Self
   end

end
