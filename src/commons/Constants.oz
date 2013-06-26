/*-------------------------------------------------------------------------
 *
 * Constants.oz
 *
 *    Static definition of default constants values.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 217 $ $Author: boriss $
 *
 *    $Date: 2010-04-12 17:23:21 +0200 (Mon, 12 Apr 2010) $
 *
 *-------------------------------------------------------------------------
 */

functor
export
   Abort
   BadSecret
   ErrorBadSec
   LargeKey
   NoAck
   NoSecret
   NotFound
   NoValue
   Public 
   Success
   SlSize
define

   Abort       = 'ABORT'      % 
   BadSecret   = bad_secret   % Incorrect secret
   ErrorBadSec = error(bad_secret) % Error: Incorrect secret
   NoAck       = nack         % Used when no remote answer is needed
   NotFound    = 'NOT_FOUND'  % To be used inside the component as constant
   Public      = public       % No secret
   Success     = 'SUCCESS'    % Correct secret, or new item created

   %% Numbers
   LargeKey    = 2097152      % 2^21 used for max key
   SlSize      = 7            % successor list size (because I like 7)

   %% aliases
   NoValue  = NotFound
   NoSecret = Public

end

