/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    Tweet.cs
 *  Desc:    Tweet data struct
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class Tweet
       |
       '-----------------------------------------------------------------------
    */
    class Tweet
    {
        public readonly long Id;
        public readonly string Text;
        public readonly DateTime CreatedAt;

        public Tweet(long id, string text, DateTime createdAt)
        {
            Id = id;
            Text = text;
            CreatedAt = createdAt;
        }
    }
}
