{-# LANGUAGE BangPatterns #-}

module Main (
  main
) where

import qualified Data.Attoparsec.ByteString as ABS
import Data.Attoparsec.ByteString.Lazy
import qualified Data.Binary.Get as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as Map
import Data.Word
import Data.Maybe

import Control.Applicative

import System.IO

import Data.Parquet.Internal.Encodings
import Data.Parquet.Internal.Enums
import Data.Parquet.Internal.DataPageTypes
import Data.Parquet.Internal.FileMetadataTypes
import Data.Parquet.Internal.Thrift.CompactProtocol
import Data.Parquet.Internal.Thrift.Types
import Data.Parquet.Internal.Thrift.Parsers

-- | Checks that the magic values are present in the file
--   (i.e. makes sure that it is actually a Parquet file)
checkMagic :: Handle -> IO Bool
checkMagic file = do 
  startPos <- hTell file
  -- check the first magic (start of file)
  hSeek file AbsoluteSeek 0
  startExists <- getMagic
  -- check the final magic (end of file)
  hSeek file SeekFromEnd (-4)
  endExists <- getMagic
  -- return the handle to the original position.
  hSeek file AbsoluteSeek startPos
  return $ startExists && endExists
    where
      magic = (BS.pack "PAR1")
      getMagic = do
        buf <- BS.hGet file 4
        return $ buf == magic

runHandle :: Handle -> BG.Get a -> IO a
runHandle handle get = do
  bytes <- BL.hGetContents handle
  return $ BG.runGet get bytes

useThriftParser :: ThriftParseable a => Parser a
useThriftParser = (TStruct <$> parseCompactStruct) >>= parseThrift


runParserUntilResultChunked :: Handle -> Int -> ABS.Parser a -> IO (Either String a)
runParserUntilResultChunked file bufferSize p = do
  initialBuffer <- getData
  initialResult <- return $ ABS.parse p initialBuffer
  iterateUntilRead initialResult
  where getData :: IO (BS.ByteString)
        getData = BS.hGet file bufferSize
        iterateUntilRead :: ABS.Result a -> IO (Either String a)
        iterateUntilRead (ABS.Fail leftOver contexts errorMessage) = return (Left errorMessage)
        iterateUntilRead (ABS.Partial f) = do 
          nextChunk  <- getData
          nextResult <- return $ f nextChunk
          iterateUntilRead nextResult
        iterateUntilRead (ABS.Done remaining result) = do
          rewindAmount <- return $ BS.length remaining
          -- return the handle to the position just after the Parser
          -- stopped.
          hSeek file RelativeSeek (fromIntegral (- rewindAmount))
          return (Right result)


defaultBufferSize = 8192
runParserUntilResult file p = runParserUntilResultChunked file defaultBufferSize p

readFileMetadata :: Handle -> IO (Either String FileMetadata)
readFileMetadata file = do
  -- first read the length (- 8 bytes from EOF).
  hSeek file SeekFromEnd (-8)
  !metadataSize <- runHandle file BG.getWord32le
  -- move to the start of the metadata pos.
  hSeek file SeekFromEnd (- (8 + (fromIntegral metadataSize) ))
  !parsed <- runParserUntilResult file useThriftParser
  return parsed

readRowGroup :: Handle -> Schema -> RowGroup -> IO ()
readRowGroup file schema rg = do
  print "================="
  print "Reading row group"
  columns <- return $ concat (maybeToList (rgColumns rg))
  sequence (map (readColumnChunk file schema) columns)
  print "================="

type Schema = Map.Map BS.ByteString SchemaElement

constructSchema :: [SchemaElement] -> Schema
constructSchema els = Map.fromList [((BL.toStrict (seName se )), se) | se <- els]

isColumnRequired :: Schema -> ColumnMetaData -> Bool
isColumnRequired schema columnMetadata =
  required $ seRepetitionType (schema Map.! (BL.toStrict (last (maybe [] id (cmdPathInSchema columnMetadata)))))
    where required (Just REQUIRED) = True
          required (Just _)        = False
          required Nothing         = True

maxDefinitionLevel :: Schema -> [BL.ByteString] -> Int
maxDefinitionLevel _ [] = 0
maxDefinitionLevel schema (x : xs) =
  (maxDefinitionLevel schema xs) + (required (seRepetitionType (schema Map.! (BL.toStrict x))))
  where required (Just REQUIRED) = 1
        required (Just _)        = 0
        required Nothing         = 1


maxRepetitionLevel :: Schema -> [BL.ByteString] -> Int
maxRepetitionLevel _ [] = 0
maxRepetitionLevel schema (x : xs) =
  (maxRepetitionLevel schema xs) + (required (seRepetitionType (schema Map.! (BL.toStrict x))))
  where required (Just REQUIRED) = 0
        required (Just _)        = 1
        required Nothing         = 0

readDefinitionLevels :: Handle -> Schema -> ColumnMetaData -> IO ()
readDefinitionLevels file schema columnMetadata = do
  if (isColumnRequired schema columnMetadata) then (print "Skipping definition levels") else readLevels
  where readLevels = do
          !definitionLength <- runHandle file BG.getWord32le
          print $ "Definition length: " ++ (show definitionLength)
          bitWidth <- return $ widthFromMaxInt (maxDefinitionLevel schema (maybe [] id (cmdPathInSchema columnMetadata)))
          !levels <- coerceEither <$> (runParserUntilResult file (readRLEBitPackingHybrid bitWidth))
          print $ (show levels)

readColumnChunk :: Handle -> Schema -> ColumnChunk -> IO ()
readColumnChunk file schema columnChunk = do
  columnMetadata <- return $ coerceMaybe (ccMetadata columnChunk)
  print "------"
  print (show columnChunk)
  print (show columnMetadata)
  offset <- return $ maybe (-1) id (cmdDataPageOffset columnMetadata)
  hSeek file AbsoluteSeek (fromIntegral offset)
  !pageHeader  <- coerceEither <$> (runParserUntilResult file pageHeaderParser)
  print (show pageHeader)
  --
  print "Reading Definition Levels"
  readDefinitionLevels file schema columnMetadata
  --
  print "Reading Repetition Levels"
  !repetitionLength <- runHandle file BG.getWord32le
  print $ "Repetition length: " ++ (show repetitionLength)
  skipBytes file (fromIntegral repetitionLength)

  print "-------"
    where pageHeaderParser :: Parser PageHeader
          pageHeaderParser = useThriftParser

skipBytes :: Handle -> Integer -> IO ()
skipBytes file count = hSeek file RelativeSeek count

readDataPage :: Handle -> FileMetadata -> IO ()
readDataPage file fm = do
  sequence $ map (readRowGroup file (constructSchema (fmSchema fm))) (fmRowGroups fm)
  return ()

coerceMaybe :: Maybe a -> a
coerceMaybe (Just a) = a

coerceEither :: Show a => Either a b -> b
coerceEither (Right b) = b
coerceEither (Left a) = error (show a)

main = do
  handle   <- openFile "customer.impala.parquet" ReadMode
  result   <- checkMagic handle
  print "Magic was valid - decoding file metadata."
  metadata <- readFileMetadata handle
  print $ show metadata
  print "Got file metadata - reading first data chunk."
  page     <- readDataPage handle (coerceEither metadata)
  print "Finished reading data page."
  hClose handle
  print "Done."
