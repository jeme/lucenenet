#if FEATURE_BREAKITERATOR
using ICU4N.Text;
using Lucene.Net.Analysis.TokenAttributes;
using Lucene.Net.Support;
using NUnit.Framework;
using System.Globalization;
using System.IO;
using System.Text;

namespace Lucene.Net.Analysis.Util
{
    /*
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /// <summary>
    /// Basic tests for <seealso cref="SegmentingTokenizerBase"/> </summary>

    [TestFixture]
    public class TestSegmentingTokenizerBase : BaseTokenStreamTestCase
    {
        public override void SetUp()
        {
            base.SetUp();
        }

        private static readonly Analyzer sentence = Analyzer.NewAnonymous(createComponents: (fieldName, reader) =>
        {
            return new TokenStreamComponents(new WholeSentenceTokenizer(reader));
        });

        private static readonly Analyzer sentenceAndWord = Analyzer.NewAnonymous(createComponents: (fieldName, reader) =>
        {
            return new TokenStreamComponents(new SentenceAndWordTokenizer(reader));
        });

        [Test]
        public virtual void TestBasics()
        {
            AssertAnalyzesTo(sentence, "The acronym for United States is U.S. but this doesn't end a sentence", new[] { "The acronym for United States is U.S. but this doesn't end a sentence" });
            AssertAnalyzesTo(sentence, "He said, \"Are you going?\" John shook his head.", new[] { "He said, \"Are you going?\" ", "John shook his head." });
        }

        [Test]
        public virtual void TestCustomAttributes()
        {
            AssertAnalyzesTo(sentenceAndWord, "He said, \"Are you going?\" John shook his head.", new[] { "He", "said", "Are", "you", "going", "John", "shook", "his", "head" }, new[] { 0, 3, 10, 14, 18, 26, 31, 37, 41 }, new[] { 2, 7, 13, 17, 23, 30, 36, 40, 45 }, new[] { 1, 1, 1, 1, 1, 2, 1, 1, 1 });
        }

        [Test]
        public virtual void TestReuse()
        {
            AssertAnalyzesTo(sentenceAndWord, "He said, \"Are you going?\"", new[] { "He", "said", "Are", "you", "going" }, new[] { 0, 3, 10, 14, 18 }, new[] { 2, 7, 13, 17, 23 }, new[] { 1, 1, 1, 1, 1 });
            AssertAnalyzesTo(sentenceAndWord, "John shook his head.", new[] { "John", "shook", "his", "head" }, new[] { 0, 5, 11, 15 }, new[] { 4, 10, 14, 19 }, new[] { 1, 1, 1, 1 });
        }

        [Test]
        public virtual void TestEnd()
        {
            // BaseTokenStreamTestCase asserts that end() is set to our StringReader's length for us here.
            // we add some junk whitespace to the end just to test it.
            AssertAnalyzesTo(sentenceAndWord, "John shook his head          ", new[] { "John", "shook", "his", "head" });
            AssertAnalyzesTo(sentenceAndWord, "John shook his head.          ", new[] { "John", "shook", "his", "head" });
        }

        [Test]
        public virtual void TestHugeDoc()
        {
            var sb = new StringBuilder();
            var whitespace = new char[4094];
            Arrays.Fill(whitespace, '\n');
            sb.Append(whitespace);
            sb.Append("testing 1234");
            var input = sb.ToString();
            AssertAnalyzesTo(sentenceAndWord, input, new[] { "testing", "1234" });
        }

        [Test]
        public virtual void TestHugeTerm()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 10240; i++)
            {
                sb.Append('a');
            }
            var input = sb.ToString();
            var token = new char[1024];
            Arrays.Fill(token, 'a');
            var expectedToken = new string(token);
            var expected = new[] { expectedToken, expectedToken, expectedToken, expectedToken, expectedToken, expectedToken, expectedToken, expectedToken, expectedToken, expectedToken };
            AssertAnalyzesTo(sentence, input, expected);
        }

        [Test]
        public virtual void TestRandomStrings()
        {
            CheckRandomData(Random, sentence, 10000 * RandomMultiplier);
            CheckRandomData(Random, sentenceAndWord, 10000 * RandomMultiplier);
        }

        // some tokenizers for testing

        /// <summary>
        /// silly tokenizer that just returns whole sentences as tokens </summary>
        sealed class WholeSentenceTokenizer : SegmentingTokenizerBase
        {
            internal int sentenceStart, sentenceEnd;
            internal bool hasSentence;

            internal ICharTermAttribute termAtt;
            internal IOffsetAttribute offsetAtt;

            public WholeSentenceTokenizer(TextReader reader)
                : base(reader, BreakIterator.GetSentenceInstance(CultureInfo.InvariantCulture))
            {
                termAtt = AddAttribute<ICharTermAttribute>();
                offsetAtt = AddAttribute<IOffsetAttribute>();
            }

            protected override void SetNextSentence(int sentenceStart, int sentenceEnd)
            {
                this.sentenceStart = sentenceStart;
                this.sentenceEnd = sentenceEnd;
                hasSentence = true;
            }

            protected override bool IncrementWord()
            {
                if (hasSentence)
                {
                    hasSentence = false;
                    ClearAttributes();
                    termAtt.CopyBuffer(m_buffer, sentenceStart, sentenceEnd - sentenceStart);
                    offsetAtt.SetOffset(CorrectOffset(m_offset + sentenceStart), CorrectOffset(m_offset + sentenceEnd));
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// simple tokenizer, that bumps posinc + 1 for tokens after a 
        /// sentence boundary to inhibit phrase queries without slop.
        /// </summary>
        sealed class SentenceAndWordTokenizer : SegmentingTokenizerBase
        {
            internal int sentenceStart, sentenceEnd;
            internal int wordStart, wordEnd;
            internal int posBoost = -1; // initially set to -1 so the first word in the document doesn't get a pos boost

            internal ICharTermAttribute termAtt;
            internal IOffsetAttribute offsetAtt;
            internal IPositionIncrementAttribute posIncAtt;

            public SentenceAndWordTokenizer(TextReader reader)
                : base(reader, BreakIterator.GetSentenceInstance(CultureInfo.InvariantCulture))
            {
                termAtt = AddAttribute<ICharTermAttribute>();
                offsetAtt = AddAttribute<IOffsetAttribute>();
                posIncAtt = AddAttribute<IPositionIncrementAttribute>();
            }

            protected override void SetNextSentence(int sentenceStart, int sentenceEnd)
            {
                this.wordStart = this.wordEnd = this.sentenceStart = sentenceStart;
                this.sentenceEnd = sentenceEnd;
                posBoost++;
            }

            public override void Reset()
            {
                base.Reset();
                posBoost = -1;
            }

            protected override bool IncrementWord()
            {
                wordStart = wordEnd;
                while (wordStart < sentenceEnd)
                {
                    if (char.IsLetterOrDigit(m_buffer[wordStart]))
                    {
                        break;
                    }
                    wordStart++;
                }

                if (wordStart == sentenceEnd)
                {
                    return false;
                }

                wordEnd = wordStart + 1;
                while (wordEnd < sentenceEnd && char.IsLetterOrDigit(m_buffer[wordEnd]))
                {
                    wordEnd++;
                }

                ClearAttributes();
                termAtt.CopyBuffer(m_buffer, wordStart, wordEnd - wordStart);
                offsetAtt.SetOffset(CorrectOffset(m_offset + wordStart), CorrectOffset(m_offset + wordEnd));
                posIncAtt.PositionIncrement = posIncAtt.PositionIncrement + posBoost;
                posBoost = 0;
                return true;
            }
        }
    }
}
#endif