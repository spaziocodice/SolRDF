# filename: ex482.ru

PREFIX v:    <http://www.w3.org/2006/vcard/ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

DELETE { ?s foaf:mbox ?o . }
INSERT { ?s v:email ?o . }
WHERE  { ?s foaf:mbox ?o . }
