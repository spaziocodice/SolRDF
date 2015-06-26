# filename: ex320.ru

PREFIX fb: <http://rdf.freebase.com/ns/>
PREFIX db: <http://dbpedia.org/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

DELETE DATA 
{
  fb:en.tommy_potter fb:people.person.date_of_birth  "1918-09-21" ;
                     owl:sameAs db:Tommy_Potter . 
}

