using System;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Lucene.Net.Util;

namespace Lucene.Net.Benchmarks
{
    class Program
    {
        static void Main(string[] args)
        {
            Summary[] summary = BenchmarkRunner.Run(typeof(Program).Assembly);
        }
    }

    
    [KeepBenchmarkFiles]
    [RPlotExporter]
    [MarkdownExporter]
    [MarkdownExporterAttribute.Default]
    [MarkdownExporterAttribute.GitHub]
    [MarkdownExporterAttribute.StackOverflow]
    [MarkdownExporterAttribute.Atlassian]
    //[SimpleJob(RuntimeMoniker.CoreRt31)]
    //[SimpleJob(RuntimeMoniker.Net472)]
    public class MathBenchmarks
    {
        private readonly double[] data;

        public MathBenchmarks()
        {
            data = Enumerable.Repeat(new Random(42), 1000).Select(rnd => rnd.NextDouble() + rnd.Next(int.MaxValue)).ToArray();
        }

        [Benchmark]
        public void MathUtil_Acosh()
        {
            foreach (double value in data)
                MathUtil.Acosh(value);
        }

        [Benchmark]
        public void Math_Acosh()
        {
            foreach (double value in data)
                Math.Acosh(value);
        }

        //[Benchmark]
        //public void MathUtil_Asinh()
        //{
        //    foreach (double value in data)
        //        MathUtil.Asinh(value);
        //}

        //[Benchmark]
        //public void Math_Asinh()
        //{
        //    foreach (double value in data)
        //        Math.Asinh(value);
        //}

        //[Benchmark]
        //public void MathUtil_Log()
        //{
        //    foreach (double value in data)
        //        MathUtil.Log(value, 10);
        //}

        //[Benchmark]
        //public void Math_Log()
        //{
        //    foreach (double value in data)
        //        Math.Log(value, 10);
        //}

        //[Benchmark]
        //public void MathUtil_Atanh()
        //{
        //    foreach (double value in data)
        //        MathUtil.Atanh(value);
        //}

        //[Benchmark]
        //public void Math_Atanh()
        //{
        //    foreach (double value in data)
        //        Math.Atanh(value);
        //}
    }
}
