# filename: ex352.ru

PREFIX ab: <http://learningsparql.com/ns/addressbook#>
PREFIX d: <http://learningsparql.com/ns/data#>
PREFIX g: <http://learningsparql.com/graphs/>

DELETE
{ GRAPH g:courses
    { d:course34 ab:courseTitle ?courseTitle }
}
INSERT 
{ GRAPH g:courses 
  { d:course34 ab:courseTitle "Modeling Data with RDFS and OWL" . } 
}
WHERE
{ GRAPH g:courses
  { d:course34 ab:courseTitle ?courseTitle }
} ;

INSERT DATA 
{ GRAPH g:courses 
  { d:course86 ab:courseTitle "Querying and Updating Named Graphs" . }
}

