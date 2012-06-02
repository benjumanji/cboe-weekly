{-# LANGUAGE OverloadedStrings #-}

module Main
  where

import Prelude hiding (catch, takeWhile)

import Text.CSV.ByteString
import Control.Applicative 
  ( (<$>)
  , (<*)
  , (*>)
  , (<*>)
  )
import Control.Arrow
import Control.Exception (catch)
import Data.Attoparsec.Char8 
  ( Parser
  , char
  , double
  , endOfLine
  , feed
  , isDigit
  , isEndOfLine
  , isAlpha_ascii
  , maybeResult
  , parse
  , scan
  , sepBy
  , takeWhile
  )
import Data.Time
import Data.Time.Format()
import Data.Time.Clock.POSIX()
import Database.HDBC
import Database.HDBC.PostgreSQL
import System.Directory(getDirectoryContents)
import System.FilePath(takeExtension, dropExtension, (</>))
import System.Locale (defaultTimeLocale)

import qualified Data.ByteString.Char8 as B
import qualified Data.Attoparsec.ByteString as W8

import Control.Concurrent.KSack.ThreadPoolWithResource
import Database.HDBC.PostgreSQL.KSack.ConnectionPool

main :: IO ()
main = do cp <- createConnectionPool 10 (connectPostgreSQL "dbname=historic_data")
          tp <- createThreadPool 10
          fs <- files
          let actions = map go fs
          mapM_ (tpQueueItem tp) actions
          tpSignalEnd tp
          tpStart tp (takeConn cp) (putConn cp) >> tpJoin tp
          disconnectAll cp
  where go fileInfo conn = catch (uncurry (loadFile conn) fileInfo)
                                 (\e -> do let msg = show (e :: SqlError) 
                                           putStrLn ("caught: " ++ msg)
                                           rollback conn)
                            
type Exchange = String
type TickerSymbol =  String 

data GoogleDataRow =
  GDR { gdrSymbol :: String
      , gdrDate   :: Day
      , gdrOpen   :: Double
      , gdrHigh   :: Double
      , gdrLow    :: Double
      , gdrClose  :: Double
      , gdrVolume :: Integer
      } deriving Show

instance Eq GoogleDataRow where
  x1 == x2 = d1 == d2
    where d1 = gdrDate x1
          d2 = gdrDate x2


comma :: Parser Char
comma = char ','

doublec :: Parser Double
doublec = double <* comma


data DateState
  = Begin
  | DayPart Int
  | Hyphen Int
  | MonthPart Int
  | YearPart Int
  | End

scanDate :: DateState -> Char -> Maybe DateState
scanDate Begin         c | isDigit c       = Just (DayPart 1)
scanDate (DayPart 1)   c | isDigit c       = Just (DayPart 2)
                         | c == '-'        = Just (Hyphen 1) 
scanDate (DayPart 2)   c | c == '-'        = Just (Hyphen 1)
scanDate (Hyphen 1)    c | isAlpha_ascii c = Just (MonthPart 1)
scanDate (MonthPart 1) c | isAlpha_ascii c = Just (MonthPart 2)
scanDate (MonthPart 2) c | isAlpha_ascii c = Just (MonthPart 3)
scanDate (MonthPart 3) c | c == '-'        = Just (Hyphen 2)
scanDate (Hyphen 2)    c | isDigit c       = Just (YearPart 1)
scanDate (YearPart 1)  c | isDigit c       = Just End 
scanDate End           _ = Nothing
scanDate _             _ = Nothing

parseDate :: Parser Day
parseDate = do s <- scan Begin scanDate
               let d = parseTime defaultTimeLocale "%e-%b-%y" . B.unpack $ s
               maybe (fail "parseDate") (return) d


integer :: Parser Integer
integer = takeWhile isDigit >>= maybe (fail "integer") (return . fst) . B.readInteger

row :: String -> Parser GoogleDataRow
row symb = GDR symb <$> (parseDate <* comma) -- date
                    <*> doublec   -- open
                    <*> doublec   -- high
                    <*> doublec   -- low
                    <*> doublec   -- close
                    <*> integer   -- volume 

funcs :: [B.ByteString -> Maybe SqlValue]
funcs = undefined

csv :: String -> Parser [GoogleDataRow]
csv symb = W8.takeWhile (not . isEndOfLine) *> endOfLine *> -- drop the first line
           row symb `sepBy` endOfLine 
           

gdrToSql :: GoogleDataRow -> [SqlValue]
gdrToSql (GDR s d o h l c v) = 
  [ toSql s 
  , toSql d
  , toSql o
  , toSql h
  , toSql l
  , toSql c
  , toSql v
  ]

parseDailyPrice :: String -> [Field] -> Maybe [SqlValue]
parseDailyPrice symb fs = sequence vals
  where vals = (Just $ toSql symb) : (zipWith ($) funcs fs)

csvDir :: String
csvDir = "/home/ben/historic/"

files :: IO [((Exchange, TickerSymbol), FilePath)]
files = do contents <- getDirectoryContents csvDir
           let fs = filter ((== ".csv") . takeExtension) contents
           return $ map (es &&& (csvDir </>)) fs
  where es = second tail . break (== '-') . dropExtension

insertSymbol :: String
insertSymbol = "insert into exchange_symbol values(?,?);"

insertPrice :: String
insertPrice  = "insert into daily_price values (?,?,?,?,?,?,?);"

loadFile :: IConnection conn => conn -> (TickerSymbol, Exchange) -> FilePath -> IO ()
loadFile conn (_, symb) path = do 
  bs <- B.readFile path
  print $ "loading: " ++ symb
--  _ <- quickQuery' conn insertSymbol (map toSql [ex,symb])
  stmt <- prepare conn insertPrice
  let p = (map gdrToSql . dedup) `fmap` (maybeResult $ feed (parse (csv symb) bs) "")
  maybe (return ()) (\x -> executeMany stmt x >> commit conn) p 
 

dedup :: [GoogleDataRow] -> [GoogleDataRow]
dedup [] = []
dedup x@([_]) = x
dedup (x:xs) = x : dedup' x xs

dedup' :: GoogleDataRow -> [GoogleDataRow] -> [GoogleDataRow]
dedup' _ []     = []
dedup' x (z:zs) = if x == z then dedup' z zs else z : dedup' z zs

