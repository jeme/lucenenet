using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Editing;
using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Lucene.Net.CodeAnalysis
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

    [ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(Lucene1000_SealIncrementTokenMethodCSCodeFixProvider)), Shared]
    public class Lucene1000_SealIncrementTokenMethodCSCodeFixProvider : CodeFixProvider
    {
        private const string Title = "Add sealed keyword to IncrementToken() method";

        public sealed override ImmutableArray<string> FixableDiagnosticIds => ImmutableArray.Create(Lucene1000_TokenStreamOrItsIncrementTokenMethodMustBeSealedCSAnalyzer.DiagnosticId);

        public sealed override FixAllProvider GetFixAllProvider()
        {
            // See https://github.com/dotnet/roslyn/blob/master/docs/analyzers/FixAllProvider.md for more information on Fix All Providers
            return WellKnownFixAllProviders.BatchFixer;
        }

        public sealed override async Task RegisterCodeFixesAsync(CodeFixContext context)
        {
            var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);

            // TODO: Replace the following code with your own analysis, generating a CodeAction for each fix to suggest
            var diagnostic = context.Diagnostics.First();
            var diagnosticSpan = diagnostic.Location.SourceSpan;

            // Find the type declaration identified by the diagnostic.
            var declaration = root.FindToken(diagnosticSpan.Start).Parent.AncestorsAndSelf().OfType<ClassDeclarationSyntax>().First();

            var incrementTokenMethodDeclaration = GetIncrementTokenMethodDeclaration(declaration);

            // If we can't find the method, we skip registration for this fix
            if (incrementTokenMethodDeclaration != null)
            {
                // Register a code action that will invoke the fix.
                context.RegisterCodeFix(
                    CodeAction.Create(
                        title: Title,
                        createChangedDocument: c => AddSealedKeywordAsync(context.Document, incrementTokenMethodDeclaration, c),
                        equivalenceKey: Title),
                    diagnostic);
            }
        }

        private async Task<Document> AddSealedKeywordAsync(Document document, MethodDeclarationSyntax methodDeclaration, CancellationToken cancellationToken)
        {
            var generator = SyntaxGenerator.GetGenerator(document);

            DeclarationModifiers modifiers = DeclarationModifiers.None;
            if (methodDeclaration.Modifiers.Any(SyntaxKind.NewKeyword))
            {
                modifiers |= DeclarationModifiers.New;
            }
            if (methodDeclaration.Modifiers.Any(SyntaxKind.OverrideKeyword))
            {
                modifiers |= DeclarationModifiers.Override;
            }
            if (methodDeclaration.Modifiers.Any(SyntaxKind.UnsafeKeyword))
            {
                modifiers |= DeclarationModifiers.Unsafe;
            }
            modifiers |= DeclarationModifiers.Sealed;

            var newMethodDeclaration = generator.WithModifiers(methodDeclaration, modifiers);

            var oldRoot = await document.GetSyntaxRootAsync(cancellationToken);
            var newRoot = oldRoot.ReplaceNode(methodDeclaration, newMethodDeclaration);

            // Return document with transformed tree.
            return document.WithSyntaxRoot(newRoot);
        }

        private MethodDeclarationSyntax GetIncrementTokenMethodDeclaration(ClassDeclarationSyntax classDeclaration)
        {
            foreach (var member in classDeclaration.Members.Where(m => m.Kind() == SyntaxKind.MethodDeclaration))
            {
                var methodDeclaration = (MethodDeclarationSyntax)member;

                if (methodDeclaration.Identifier.ValueText == "IncrementToken")
                {
                    return methodDeclaration;
                }
            }
            return null;
        }
    }
}
