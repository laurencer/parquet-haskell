{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Parquet.Internal.DataPageTypes(
  PageHeader(..)
) where

import Control.Applicative

import           Data.Attoparsec.Internal.Types (Parser)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as Map
import           Data.Int

import           Data.Parquet.Internal.Enums
import           Data.Parquet.Internal.FileMetadataTypes
import           Data.Parquet.Internal.Thrift.Types
import           Data.Parquet.Internal.Thrift.Parsers


data PageHeader = PageHeader {
  phPageType                  :: PageType,
  phUncompressedPageSize      :: Int32,
  phCompressedPageSize        :: Int32,
  phCrc                       :: Maybe Int32,

  phDataPageHeader            :: Maybe DataPageHeader,
  phIndexPageHeader           :: Maybe DataPageHeader,
  phDictionaryPageHeader      :: Maybe DataPageHeader,
  phDataPageHeaderV2          :: Maybe DataPageHeader
} deriving (Eq, Ord, Show)

instance ThriftParseable PageHeader where
  parseThrift tv = do
    p                       <- asStruct tv
    phPageType              <- (readField p 1) >>= asEnum
    phUncompressedPageSize  <- (readField p 2) >>= required >>= parseThrift
    phCompressedPageSize    <- (readField p 3) >>= required >>= parseThrift
    phCrc                   <- (readField p 4) >>= asMaybeParseThrift

    phDataPageHeader        <- (readField p 5) >>= asMaybeParseThrift
    phIndexPageHeader       <- (readField p 6) >>= asMaybeParseThrift
    phDictionaryPageHeader  <- (readField p 7) >>= asMaybeParseThrift
    phDataPageHeaderV2      <- (readField p 8) >>= asMaybeParseThrift
    return $ PageHeader {..}


data DataPageHeader = DataPageHeader {
  dphNumValues                :: Int,
  dphEncoding                 :: Encoding,
  dphDefinitionLevelEncoding  :: Encoding,
  dphReptitionLevelEncoding   :: Encoding,
  dphStatistics               :: Maybe Statistics
} deriving (Eq, Ord, Show)

instance ThriftParseable DataPageHeader where
  parseThrift tv = do
    p                           <- asStruct tv
    dphNumValues                <- (readField p 1) >>= required >>= parseThrift
    dphEncoding                 <- (readField p 2) >>= asEnum
    dphDefinitionLevelEncoding  <- (readField p 3) >>= asEnum
    dphReptitionLevelEncoding   <- (readField p 4) >>= asEnum
    dphStatistics               <- (readField p 5) >>= asMaybeParseThrift
    return $ DataPageHeader {..}
