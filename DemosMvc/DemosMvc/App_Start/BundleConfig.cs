using System.Web.Optimization;

namespace DemosMvc
{
    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new ScriptBundle("~/Scripts/jquery1.9.1").Include(
                        "~/Scripts/jquery1.9.1.js",
                        "~/Scripts/jquery.mousestop.my.js"));

            bundles.Add(new ScriptBundle("~/Scripts/bootstrap2.3.1").Include(
                        "~/Scripts/bootstrap2.3.1.my.js",
                        "~/Scripts/bootstrap-datetimepicker0.0.9.my.js"));

            bundles.Add(new ScriptBundle("~/Scripts/highstock1.3.0").Include(
                        "~/Scripts/highstock1.3.0.my.js"));

            bundles.Add(new ScriptBundle("~/Scripts/common").Include(
                        "~/Scripts/common.js"));

            bundles.Add(new ScriptBundle("~/Scripts/TwitterSentimentIndexDemo").Include(
                        "~/Scripts/TwitterSentimentIndexDemo.js"));

            bundles.Add(new StyleBundle("~/Content/css/bootstrap2.3.1").Include(
                        "~/Content/css/Bootstrap/bootstrap2.3.1.my.css",
                        "~/Content/css/Bootstrap/bootstrap-responsive2.3.1.css",
                        "~/Content/css/Bootstrap/bootstrap-datetimepicker0.0.9.my.css"));

            bundles.Add(new StyleBundle("~/Content/css/demo").Include(
                        "~/Content/css/demo.css"));
        }
    }
}