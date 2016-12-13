# googlecloud-shapeless
[ ![Codeship Status for emartech/googlecloud-shapeless](https://app.codeship.com/projects/40ea3600-7e64-0134-56d1-42c59cf9d92c/status?branch=master)](https://app.codeship.com/projects/181633)

# Formatting for google cloud services without boilerplate

## Overview


Google provides different APIs in different packages, unfortunately some libraries depend on one while others depend on the other API. 

Our primary goal is to provide boilerplate free serialization for immediate access of google cloud resources and for Google cloud dataflow via the SCIO library created by spotify. 

This project only provides the formatting for case classes, to authenticate commnicate with google please look for other referred libraries. 

Currently we aim at 
- BigQuery tables
- DataStore entities

## Roadmap

Our roadmap is based on our own needs at the moment. 

- finalize project structure use traits for formats etc.

- Datastore / Datastore v1 enetities support all datatypes, and different date format, finalize, include embedded records

- BigQuery add v1 and finalize support all datatypes including embedded records

- add support for BigTable and other services as needed

## Setup

Usage in sbt:

```"com.emarsys" %% "googlecloud-shapeless"  % "0.0.1.6" ```

## Getting Started

For using the formats simply import the corresponding objects internals and the syntax object: 
```
import com.emarsys.shapeless.googlecloud.formats.DataStore._
import com.emarsys.shapeless.googlecloud.formats.syntax._
  ```
  
Now for any case class you have access to the following two methods:
```
val enitiy =  caseClassInstance.toEntity("project-id", "namespace", "kind") // you must provide the parameters for key generation

val parsedEvent = entity.parseTo[CaseClass] 
```

Similarly for BigQuery you need 
```
import com.emarsys.shapeless.googlecloud.formats.BigQuery._
import com.emarsys.shapeless.googlecloud.formats.syntax._

```

And you'l have 
```
val row = caseClassInstance.toTableRow
val parsedRow = row.as[CaseClass]
```


## Supported types
 - Int, 
 - Long, 
 - Double, 
 - String, 
 - org.joda.time.DateTime (formatted as long timestamp)
 
more to come...

## Format type classes

You should add or override the instances of these case classes to wirk with any given type ```T```

Values are used for simple types (Ints etc.) supported within case classes. 


```
 trait BigQueryFormat[T] {
    def fromTableRow(tr: TableRow): FormatResult[T]
    def toTableRow(t: T): TableRow
  }

  trait BigQueryValue[T] {
    def toValue(v: T): AnyRef
    def fromValue(v: AnyRef): T
  }

  trait DataStoreV1Format[T] {
    def toEntity(namespace: String, kind: String, keyFields: List[String])(t: T): EntityV1
    def parseEntity(e: EntityV1): T
  }

  trait EntityValueV1[T] {
    def toValue(t: T): ValueV1
    def fromValue(v: ValueV1): T
  }

  trait DataStoreFormat[T] {
    def toEntity(projectId: String, namespace: String, kind: String)(t: T): Entity
    def parseEntity(e: Entity): T
  }

  trait EntityValue[T] {
    type ResultType
    def toValue(t: T): Value[ResultType]
    def fromValue(v: Value[ResultType]): T
  }
```

## Caveats

First of all this is not mature, but an  EXPRIMENTAL library, though we use parts of it in production

 - everything in this lib is subject to change. 

There are a few 
 - embedded types/records are not supported yet
 - DateTimes are translated to long instances accepted by Google services, but they will be stored as long values. 
 - Ints are widened to Longs in DataStore
 
## Authors

Gyorgy Farkas 
Margit Fawal
Vilmos Feher
Balazs Kiss
Andras Papp

