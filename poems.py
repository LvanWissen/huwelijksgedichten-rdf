import datetime
from collections import defaultdict
from itertools import count

import rdflib
from rdflib import Dataset, Graph, URIRef, Literal, XSD, Namespace, RDFS, BNode, OWL
from rdfalchemy import rdfSubject, rdfMultiple, rdfSingle

import pandas as pd

DATA = pd.read_csv('data/gedichtenGGD_STCN_Steur_stripped.csv', sep=';')

create = Namespace("https://data.create.humanities.uva.nl/")
schema = Namespace("https://schema.org/")
bio = Namespace("http://purl.org/vocab/bio/0.1/")
foaf = Namespace("http://xmlns.com/foaf/0.1/")
void = Namespace("http://rdfs.org/ns/void#")
dcterms = Namespace("http://purl.org/dc/terms/")

rdflib.graph.DATASET_DEFAULT_GRAPH_ID = create

ga = Namespace("https://data.create.humanities.uva.nl/id/datasets/huwelijksgedichten/")


class Entity(rdfSubject):
    rdf_type = URIRef('urn:entity')

    label = rdfMultiple(RDFS.label)
    name = rdfMultiple(schema.name)

    mainEntityOfPage = rdfSingle(schema.mainEntityOfPage)
    sameAs = rdfMultiple(OWL.sameAs)

    disambiguatingDescription = rdfSingle(schema.disambiguatingDescription)

    depiction = rdfSingle(foaf.depiction)
    subjectOf = rdfMultiple(schema.subjectOf)
    about = rdfSingle(schema.about)
    url = rdfSingle(schema.url)

class DatasetClass(Entity):

    # db = ConjunctiveGraph

    rdf_type = void.Dataset, schema.Dataset

    title = rdfMultiple(dcterms.title)
    description = rdfMultiple(dcterms.description)
    creator = rdfMultiple(dcterms.creator)
    publisher = rdfMultiple(dcterms.publisher)
    contributor = rdfMultiple(dcterms.contributor)
    source = rdfSingle(dcterms.source)
    date = rdfSingle(dcterms.date)
    created = rdfSingle(dcterms.created)
    issued = rdfSingle(dcterms.issued)
    modified = rdfSingle(dcterms.modified)

    exampleResource = rdfSingle(void.exampleResource)
    vocabulary = rdfMultiple(void.vocabulary)
    triples = rdfSingle(void.triples)

class CreativeWork(Entity):
    rdf_type = schema.CreativeWork

    publication = rdfMultiple(schema.publication)
    author = rdfMultiple(schema.author)

    about = rdfSingle(schema.about)


class PublicationEvent(Entity):
    rdf_type = schema.PublicationEvent

    startDate = rdfSingle(schema.startDate)
    location = rdfSingle(schema.location)

    publishedBy = rdfMultiple(schema.publishedBy)


class Place(Entity):
    rdf_type = schema.Place


class Marriage(Entity):
    rdf_type = bio.Marriage

    date = rdfSingle(bio.date)
    partner = rdfMultiple(bio.partner)
    place = rdfSingle(bio.place)

    subjectOf = rdfMultiple(schema.subjectOf)


class Person(Entity):
    rdf_type = schema.Person


def main(data, g):

    personCounter = count(1)
    marriageCounter = count(1)
    documentCounter = count(1)

    persons = dict()
    marriages = dict()

    marriage2records = defaultdict(list)

    for r in data.to_dict(orient='records'):

        groom = persons.get(r['Bruidegom'], None)
        if not groom:
            groom = Person(ga.term('Person/' + str(next(personCounter))),
                           label=[r['Bruidegom']],
                           name=[r['Bruidegom']])
            persons[r['Bruidegom']] = groom

        bride = persons.get(r['Bruid'], None)
        if not bride:
            bride = Person(ga.term('Person/' + str(next(personCounter))),
                           label=[r['Bruid']],
                           name=[r['Bruid']])
            persons[r['Bruid']] = bride

        partners = [groom, bride]

        if pd.isna(r['Plaats_huwelijk']):
            place = None
        else:
            place = Place(ga.term('Place/' + "".join([
                i for i in r['Plaats_huwelijk']
                if i.lower() in 'abcdefghijklmnopqrstuvwxyz'
            ])),
                          label=[r['Plaats_huwelijk']])

        if pd.isna(r['Plaats_druk']):
            placePrint = None
        else:
            placePrint = Place(ga.term('Place/' + "".join([
                i for i in r['Plaats_druk']
                if i.lower() in 'abcdefghijklmnopqrstuvwxyz'
            ])),
                               label=[r['Plaats_druk']])

        if pd.isna(r['Jaar']):
            date = None
        else:
            date = Literal(int(r['Jaar']), datatype=XSD.gYear, normalize=False)

        marriagelabel = [
            f"Huwelijk tussen {groom.label[0]} en {bride.label[0]} ({date if date else '?'})"
        ]

        marriage = marriages.get(marriagelabel[0], None)
        if not marriage:
            marriage = Marriage(ga.term('Marriage/' +
                                        str(next(marriageCounter))),
                                date=date,
                                partner=partners,
                                place=place,
                                label=marriagelabel)
            marriages[marriagelabel[0]] = marriage

        if pd.isna(r['Drukker']):
            printer = []
        else:
            printer = persons.get(r['Drukker'], None)
            if not printer:
                printer = Person(ga.term('Person/' + str(next(personCounter))),
                                 label=[r['Drukker']],
                                 name=[r['Drukker']])
                persons[r['Drukker']] = printer

            printer = [printer]

        authors = []
        for i in r:
            if i.startswith('Auteur') and not pd.isna(r[i]):

                author = persons.get(r[i], None)
                if not author:
                    author = Person(ga.term('Person/' +
                                            str(next(personCounter))),
                                    label=[r[i]],
                                    name=[r[i]])
                    persons[r[i]] = author
                authors.append(author)

        pubevent = PublicationEvent(
            None,
            startDate=date,
            publishedBy=printer,
            location=placePrint,
            label=[
                f"Gedrukt door {printer[0].label[0] if printer else 'Onbekend'}, {placePrint.label[0] if placePrint else ''} {date}"
            ])

        documentNumber = str(next(documentCounter))
        document = CreativeWork(
            ga.term('Document/' + documentNumber),
            publication=[pubevent],
            author=authors,
            about=marriage,
            label=[
                f"Gedicht op het huwelijk van {groom.label[0]} en {bride.label[0]}, {author.label[0]} {date if date else ''}"
            ])

        marriage2records[marriage].append(document)

    for marriage, documents in marriage2records.items():
        marriage.subjectOf = documents

    return document # exampleResource

if __name__ == "__main__":

    ds = Dataset()
    g = rdfSubject.db = ds.graph(identifier="https://data.create.humanities.uva.nl/id/datasets/huwelijksgedichten/")

    exampleResource = main(data=DATA, g=g)
    
    rdfSubject.db = ds

    description = """"""
    contributors = ""

    dataset = DatasetClass(
        URIRef("https://data.create.humanities.uva.nl/id/datasets/huwelijksgedichten/"),
        name=[
            Literal("Huwelijksgedichten", lang='nl')
        ],
        about=None,
        url=None,
        description=[Literal(description, lang='nl')],
        creator=[],
        publisher=[],
        contributor=[Literal(i) for i in contributors.split(', ')],
        source=None,
        date=Literal(datetime.datetime.now().isoformat(),
                     datatype=XSD.datetime),
        created=None,
        issued=None,
        modified=None,
        exampleResource=exampleResource,
        vocabulary=[URIRef("https://schema.org/")],
        triples=sum(1 for i in ds.graph(identifier="https://data.create.humanities.uva.nl/id/datasets/huwelijksgedichten/").subjects()))

    ds.bind('owl', OWL)
    ds.bind('dcterms', dcterms)
    ds.bind('create', create)
    ds.bind('schema', schema)
    ds.bind('void', void)
    ds.bind('foaf', foaf)

    print("Serializing!")
    ds.serialize('data/huwelijksgedichten.trig', format='trig')

    

