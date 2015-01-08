# filename: ex351.ru

PREFIX ab: <http://learningsparql.com/ns/addressbook#>
PREFIX d:  <http://learningsparql.com/ns/data#> 
PREFIX g:  <http://learningsparql.com/graphs/>

DROP ALL;

INSERT DATA 
{ 

  # people
  GRAPH g:people
  {
    d:i0432 ab:firstName "Richard" ; 
            ab:lastName  "Mutt" ; 
            ab:email     "richard49@hotmail.com" . 

    d:i9771 ab:firstName "Cindy" ; 
            ab:lastName  "Marshall" ; 
            ab:email     "cindym@gmail.com" . 

    d:i8301 ab:firstName "Craig" ; 
            ab:lastName  "Ellis" ; 
            ab:email     "c.ellis@usairwaysgroup.com" . 
  }

  # courses
  GRAPH g:courses
  {
    d:course34 ab:courseTitle "Modeling Data with OWL" .
    d:course71 ab:courseTitle "Enhancing Websites with RDFa" .
    d:course59 ab:courseTitle "Using SPARQL with non-RDF Data" .
    d:course85 ab:courseTitle "Updating Data with SPARQL" .
  }

  # who's taking which courses

  GRAPH g:enrollment
  {
    d:i8301 ab:takingCourse d:course59 . 
    d:i9771 ab:takingCourse d:course34 . 
    d:i0432 ab:takingCourse d:course85 . 
    d:i0432 ab:takingCourse d:course59 . 
    d:i9771 ab:takingCourse d:course59 . 
  }
}
