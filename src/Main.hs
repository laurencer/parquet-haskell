{-# LANGUAGE BangPatterns #-}

module Main (
  main
) where

import qualified Data.Attoparsec.ByteString as ABS
import Data.Attoparsec.ByteString.Lazy
import qualified Data.Binary.Get as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.Word
import Data.Maybe

import Control.Applicative

import System.IO

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

readRowGroup :: Handle -> RowGroup -> IO ()
readRowGroup file rg = do
  print "================="
  print "Reading row group"
  columns <- return $ concat (maybeToList (rgColumns rg))
  sequence (map (readColumnChunk file) columns)
  print "================="

readColumnChunk :: Handle -> ColumnChunk -> IO ()
readColumnChunk file columnChunk = do
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
  !definitionLength <- runHandle file BG.getWord32le
  print $ "Definition length: " ++ (show definitionLength)
  skipBytes file (fromIntegral definitionLength)
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
  sequence $ map (readRowGroup file) (fmRowGroups fm)
  return ()

coerceMaybe :: Maybe a -> a
coerceMaybe (Just a) = a

coerceEither :: Either a b -> b
coerceEither (Right b) = b

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
