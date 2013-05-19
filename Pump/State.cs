/*==========================================================================;
 *
 *  (c) Sowa Labs. All rights reserved.
 *
 *  File:    State.cs
 *  Desc:    Topic detection and tracking state
 *  Created: Feb-2013
 *
 *  Author:  Miha Grcar
 *
 ***************************************************************************/

using System;
using System.Linq;
using System.Collections.Generic;
using Latino;
using Latino.TextMining;
using Latino.Model;

namespace TwitterMonitorPump
{
    /* .-----------------------------------------------------------------------
       |
       |  Class State
       |
       '-----------------------------------------------------------------------
    */
    public class State : ISerializable
    {
        public Queue<DateTime> mTimeStamps
            = new Queue<DateTime>();
        public IncrementalBowSpace mBowSpace;
        public IncrementalKMeansClustering mClustering;

        public State(BinarySerializer reader) : this(-1, null)
        {
            Load(reader);
        }

        public State(double clusterQualityThresh, IEnumerable<string> taskStopWords)
        {
            mBowSpace = Utils.CreateBowSpace(taskStopWords);
            mClustering = Utils.CreateClustering(clusterQualityThresh);
            // *** for debugging only
            mClustering.BowSpace = mBowSpace;
            //mClustering.Logger.LocalLevel = Logger.Level.Trace;
            mClustering.Logger.LocalLevel = Logger.Level.Off;
        }

        public void Save(BinarySerializer writer)
        {
            new ArrayList<long>(mTimeStamps.Select(x => x.ToBinary())).Save(writer);
            mBowSpace.Save(writer);
            mClustering.Save(writer);
        }

        public void Load(BinarySerializer reader)
        {
            mTimeStamps = new Queue<DateTime>(new ArrayList<long>(reader).Select(x => DateTime.FromBinary(x)));
            mBowSpace.Load(reader);
            mClustering.Load(reader);
        }
    }
}
