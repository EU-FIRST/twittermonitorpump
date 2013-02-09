using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.ServiceModel.Web;
using System.Web;

namespace TwitterMonitorDAL
{
    public class ParameterChecker
    {
        public enum WindowSizeParam
        {
            D,
            W,
            M
        }

        private static List<EntityInfo> allEntities;
        private static List<EntityInfo> AllEntities
        {
            get
            {
                if (allEntities == null)
                    allEntities = new TwitterMonitor().AllEntities();
                return allEntities;
            }
        }

        public static string Entity(string entity, string windowSize)
        {
            if (AllEntities.Any(ent => 
                ent.Entity.ToLower() == entity.ToLower() && 
                ent.WindowSize.ToLower() == windowSize.ToLower()
                ))
                return entity.ToUpper();

            throw new WebFaultException<string>(
                string.Format("The specify entity ({0}) with a combination with the specified windowSize ({1}). " +
                              "Use the provided services to check all available entity-windowSize combinations." +
                              entity, windowSize),
                HttpStatusCode.NotAcceptable);
        }

        public static string WindowSize(string windowSize)
        {
            WindowSizeParam ws;
            if (Enum.TryParse(windowSize, true, out ws))
                return ws.ToString();

            return WindowSizeParam.W.ToString();
        }

        public static int PositiveNumber(int maxNumTopics, int defaultValue)
        {
            if (maxNumTopics < 0)
                return defaultValue;
            
            return maxNumTopics;
        }

        public static int StrictlyPositiveNumber(int maxNumTopics, int defaultValue)
        {
            if (maxNumTopics <= 0)
                return defaultValue;
            
            return maxNumTopics;
        }

        public static int FilterFlagCheck(int filterFlag)
        {
            return Math.Max((int)filterFlag, (int)FilterFlag.TermUnigram);
        }

        public static TimeSpan StepTimeSpan(TimeSpan stepTimeSpan)
        {
            return new TimeSpan((int) Math.Max(Math.Round(stepTimeSpan.TotalHours), 1), 0, 0);
        }

        public static DateTime DateRound(DateTime dateTime)
        {
            if (dateTime == DateTime.MinValue) 
                dateTime = DateTime.Now;
            return DateTime.MinValue + new TimeSpan((int)(Math.Round((dateTime - DateTime.MinValue).TotalHours)), 0, 0);
        }

        public static bool Boolean(bool groupedZeroPadding)
        {
            return groupedZeroPadding;
        }

        public static void CheckTimeSlotNum(DateTime dateTimeStart, DateTime dateTimeEnd, TimeSpan stepTimeSpan, int maxNumTopics)
        {
            int pointNum = (int)(maxNumTopics * (dateTimeEnd - dateTimeStart).TotalHours / stepTimeSpan.TotalHours);
            int maxPoints = 100000;
            if (pointNum > maxPoints)
                throw new WebFaultException<string>(
                    string.Format("Maximum number of data points is more than allowed ({0}). " +
                                  "The number of data points is calculated by maxNumTopics * (number of stepTimeSpan between dateTimeStart and dateTimeEnd). " +
                                  "Please set these parameters so that they do not exceed the limits. " +
                                  "Current values are: " +
                                  "maxNumTopics={1}, " +
                                  "stepTimeSpan={2}, " +
                                  "dateTimeStart={3}, " +
                                  "dateTimeEnd={4}." +
                                  "Which totals to {5} time points.",
                                  maxPoints, maxNumTopics, stepTimeSpan, dateTimeStart, dateTimeEnd, pointNum),
                    HttpStatusCode.NotAcceptable);
        }
    }
}