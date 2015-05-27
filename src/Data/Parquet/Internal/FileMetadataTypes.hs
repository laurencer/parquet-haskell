{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Parquet.Internal.FileMetadataTypes(
    FileMetadata(..)
  , RowGroup(..)
  , ParquetType
  , ConvertedType
  , ColumnChunk(..)
  , ColumnMetaData(..)
  , SortingColumn
  , KeyValue
  , Statistics
  , PageEncodingStats
  , PageType
  , CompressionCodec
  , Encoding
  , FieldRepetitionType
  , SchemaElement(..)
) where

import Control.Applicative

import           Data.Attoparsec.Internal.Types (Parser)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import           Data.Int

import           Data.Parquet.Internal.Enums
import           Data.Parquet.Internal.Thrift.Types
import           Data.Parquet.Internal.Thrift.Parsers

data FileMetadata = FileMetadata {
  fmVersion :: Int,
  fmSchema :: [SchemaElement],
  fmRowCount :: Int,
  fmRowGroups :: [RowGroup],
  fmKeyValueMetadata :: Maybe [KeyValue],
  fmCreatedBy :: Maybe LBS.ByteString
} deriving (Eq, Ord, Show)

instance ThriftParseable FileMetadata where
  parseThrift tv = do
    p                  <- asStruct tv
    fmVersion          <- (readField p 1) >>= required >>= parseThrift
    fmSchema           <- (readField p 2) >>= required >>= asParsedList
    fmRowCount         <- (readField p 3) >>= required >>= parseThrift
    fmRowGroups        <- (readField p 4) >>= required >>= asParsedList
    fmKeyValueMetadata <- (readField p 5) >>= asMaybeParsedList
    fmCreatedBy        <- (readField p 6) >>= asMaybeParseThrift
    return $ FileMetadata {..}

data SchemaElement = SchemaElement {
  seType :: Maybe ParquetType,
  seTypeLength :: Maybe Int,
  seRepetitionType :: Maybe FieldRepetitionType,
  seName :: LBS.ByteString,
  seChildCount :: Maybe Int,
  seConvertedType :: Maybe ConvertedType,
  seScale :: Maybe Int,
  sePrecision :: Maybe Int,
  seFieldId :: Maybe Int
} deriving (Show, Eq, Ord)

instance ThriftParseable SchemaElement where
  parseThrift tv = do
    p                 <- asStruct tv
    seType            <- (readField p 1) >>= asMaybeEnum
    seTypeLength      <- (readField p 2) >>= asMaybeParseThrift
    seRepetitionType  <- (readField p 3) >>= asMaybeEnum
    seName            <- (readField p 4) >>= required >>= parseThrift
    seChildCount      <- (readField p 5) >>= asMaybeParseThrift
    seConvertedType   <- (readField p 6) >>= asMaybeEnum
    seScale           <- (readField p 7) >>= asMaybeParseThrift
    sePrecision       <- (readField p 8) >>= asMaybeParseThrift
    seFieldId         <- (readField p 9) >>= asMaybeParseThrift
    return $ SchemaElement {..}

data RowGroup = RowGroup {
  rgColumns :: Maybe [ColumnChunk],
  rgTotalByteSize :: Maybe Int,
  rgRowCount :: Maybe Int,
  rgSortingColumns :: Maybe [SortingColumn]
} deriving (Show, Eq, Ord)

instance ThriftParseable RowGroup where
  parseThrift tv = do
    p                 <- asStruct tv
    rgColumns         <- (readField p 1) >>= asMaybeParsedList
    rgTotalByteSize   <- (readField p 2) >>= asMaybeParseThrift
    rgRowCount        <- (readField p 3) >>= asMaybeParseThrift
    rgSortingColumns  <- (readField p 4) >>= asMaybeParsedList
    return $ RowGroup {..}

data KeyValue = KeyValue {
  kvKey   :: Maybe LBS.ByteString,
  kvValue :: Maybe LBS.ByteString
} deriving (Show, Eq, Ord)

instance ThriftParseable KeyValue where
  parseThrift tv = do
    p       <- asStruct tv
    kvKey   <- (readField p 1) >>= asMaybeParseThrift
    kvValue <- (readField p 2) >>= asMaybeParseThrift
    return $ KeyValue {..}

data SortingColumn = SortingColumn {
  scColumnIndex :: Maybe Int,
  scIsDescending :: Maybe Bool,
  scNullsFirst :: Maybe Bool
} deriving (Show, Eq, Ord)

instance ThriftParseable SortingColumn where
  parseThrift tv = do 
    p <- asStruct tv
    scColumnIndex  <- (readField p 1) >>= asMaybeParseThrift
    scIsDescending <- (readField p 2) >>= asMaybeBool
    scNullsFirst   <- (readField p 2) >>= asMaybeBool
    return $ SortingColumn {..}

data ColumnChunk = ColumnChunk {
  ccFilePath :: Maybe LBS.ByteString,
  ccFileOffset :: Maybe Int,
  ccMetadata :: Maybe ColumnMetaData
} deriving (Show, Eq, Ord)

instance ThriftParseable ColumnChunk where
  parseThrift tv = do
    p            <- asStruct tv
    ccFilePath   <- (readField p 1) >>= asMaybeParseThrift
    ccFileOffset <- (readField p 2) >>= asMaybeParseThrift
    ccMetadata   <- (readField p 3) >>= asMaybeParseThrift
    return $ ColumnChunk {..}

data ColumnMetaData = ColumnMetaData {
  cmdType :: Maybe ParquetType,
  cmdEncodings :: Maybe [Encoding],
  cmdPathInSchema :: Maybe [LBS.ByteString],
  cmdCodec :: Maybe CompressionCodec,
  cmdValueCount :: Maybe Int,
  cmdTotalUncompressedSize :: Maybe Int,
  cmdTotalCompressedSize :: Maybe Int,
  cmdKeyValueMetadata :: Maybe [KeyValue],
  cmdDataPageOffset :: Maybe Int,
  cmdIndexPageOffset :: Maybe Int,
  cmdDictionaryPageOffset :: Maybe Int,
  cmdStatistics :: Maybe Statistics,
  cmdEncodingStats :: Maybe [PageEncodingStats]
} deriving (Show, Eq, Ord)

instance ThriftParseable ColumnMetaData where
  parseThrift tv = do
    p                         <- asStruct tv
    cmdType                   <- (readField p 1)  >>= asMaybeEnum
    cmdEncodings              <- (readField p 2)  >>= asMaybeEnumList
    cmdPathInSchema           <- (readField p 3)  >>= asMaybeParsedList
    cmdCodec                  <- (readField p 4)  >>= asMaybeEnum
    cmdValueCount             <- (readField p 5)  >>= asMaybeParseThrift
    cmdTotalUncompressedSize  <- (readField p 6)  >>= asMaybeParseThrift
    cmdTotalCompressedSize    <- (readField p 7)  >>= asMaybeParseThrift
    cmdKeyValueMetadata       <- (readField p 8)  >>= asMaybeParsedList
    cmdDataPageOffset         <- (readField p 9)  >>= asMaybeParseThrift
    cmdIndexPageOffset        <- (readField p 10) >>= asMaybeParseThrift
    cmdDictionaryPageOffset   <- (readField p 11) >>= asMaybeParseThrift
    cmdStatistics             <- (readField p 12) >>= asMaybeParseThrift
    cmdEncodingStats          <- (readField p 13) >>= asMaybeParsedList
    return $ ColumnMetaData {..}

data Statistics = Statistics {
  statsMax :: Maybe LBS.ByteString,
  statsMin :: Maybe LBS.ByteString,
  statsNullCount :: Maybe Int,
  statsDistinctCount :: Maybe Int
} deriving (Show, Eq, Ord)

instance ThriftParseable Statistics where
  parseThrift tv = do
    p                   <- asStruct tv
    statsMax            <- (readField p 1) >>= asMaybeParseThrift
    statsMin            <- (readField p 2) >>= asMaybeParseThrift
    statsNullCount      <- (readField p 3) >>= asMaybeParseThrift
    statsDistinctCount  <- (readField p 4) >>= asMaybeParseThrift
    return $ Statistics {..}

data PageEncodingStats = PageEncodingStats {
  pesPageType :: Maybe PageType,
  pesEncoding :: Maybe Encoding,
  pesCount :: Maybe Int
} deriving (Show, Eq, Ord)

instance ThriftParseable PageEncodingStats where
  parseThrift tv = do
    p           <- asStruct tv
    pesPageType <- (readField p 1) >>= asMaybeEnum
    pesEncoding <- (readField p 2) >>= asMaybeEnum
    pesCount    <- (readField p 3) >>= asMaybeParseThrift
    return $ PageEncodingStats {..}
