CREATE FULLTEXT INDEX ON [Production].[ProductReview]
    ([Comments] LANGUAGE 1033)
    KEY INDEX [PK_ProductReview_ProductReviewID]
    ON [AW2014FullTextCatalog];


GO
CREATE FULLTEXT INDEX ON [Production].[Document]
    ([DocumentSummary] LANGUAGE 1033, [Document] TYPE COLUMN [FileExtension] LANGUAGE 1033)
    KEY INDEX [PK_Document_DocumentNode]
    ON [AW2014FullTextCatalog];


GO
CREATE FULLTEXT INDEX ON [HumanResources].[JobCandidate]
    ([Resume] LANGUAGE 1033)
    KEY INDEX [PK_JobCandidate_JobCandidateID]
    ON [AW2014FullTextCatalog];

