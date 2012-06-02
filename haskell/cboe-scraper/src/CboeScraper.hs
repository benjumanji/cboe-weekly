{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Main 
  where

import qualified Data.ByteString.Char8 as B
import qualified Data.Conduit as C
import qualified Data.Text.Lazy as LT

import Control.Arrow (second)
import Control.Concurrent (threadDelay)
import Control.Exception.Base (catch)
import Data.Conduit.Binary (sinkFile)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.Text (pack, unpack)
import Data.Text.Template
import Data.Time (Day)
import Data.Time.Calendar (addDays, fromGregorian)
import Data.Time.Clock (getCurrentTime, utctDay)
import Data.Time.Format (formatTime, parseTime)
import Database.HDBC (fromSql, quickQuery', IConnection)
import Database.HDBC.PostgreSQL (connectPostgreSQL)
import Network.HTTP.Base (urlEncode)
import Network.HTTP.Conduit
import System.Console.GetOpt
import System.Environment (getArgs)
import System.Exit (ExitCode (..), exitWith)
import System.FilePath ((</>))
import System.IO.Error ()
import System.Locale (defaultTimeLocale)  
import Text.CSV.ByteString (parseCSV)

import Prelude hiding (catch)

data Flags = Flags { flgVerbose   :: Bool
                   , flgStartDate :: Day
                   , flgEndDate   :: Day
                   , flgOutDir    :: String
                   }

getDefaults :: IConnection conn => conn -> IO Flags 
getDefaults conn = do start <- addDays 1 `fmap` getMaxDate conn
                      end   <- utctDay `fmap` getCurrentTime
                      return $ Flags False start end "/home/ben/historic"

options :: [OptDescr (Flags -> Flags)]
options = 
  [ Option ['v'] ["verbose"]   
      (NoArg 
        (\f -> f { flgVerbose = True })) 
    "verbose logging"
  , Option ['s'] ["startdate"] 
      (OptArg 
        (\d f -> maybe f (\x -> f { flgStartDate = x }) . maybeDay $ d)
      "DATE")  
    "start date"
  , Option ['e'] ["enddate"]   
      (OptArg
        (\d f -> maybe f (\x -> f { flgEndDate = x }) . maybeDay $ d)
      "DATE")
    "end date"
  , Option ['o'] ["outdir"]
      (ReqArg
        (\p f -> f { flgOutDir = p })
      "DIR")
    "output dir"
  ]

maybeDay :: Maybe String -> Maybe Day
maybeDay x =  parseTime defaultTimeLocale "%d-%b-%Y" =<< x 

opts :: [String] -> IO ([Flags -> Flags], [String])
opts args = case getOpt RequireOrder options args of
              (o,n,[]) -> return (o,n)
              _        -> exitWith (ExitFailure 1)

main :: IO ()
main = do args <- getArgs
          conn <- connectPostgreSQL "dbname=historic_data"
          defaults <- getDefaults conn
          (actions, _) <- opts args
          let flags = foldl (flip ($)) defaults actions
          let path = flgOutDir flags
          let startdate = flgStartDate flags
          let action = mapM_ (tryFetch path) . map (mkContext startdate) . concat
          file <- B.readFile "/home/ben/historic/exchanges/cboe_options.csv"
          maybe (error "this is lame") action . parseCSV $ file

tryFetch :: FilePath -> Context -> IO ()
tryFetch p x = go 0
  where action w = do threadDelay (backoff w) >> fetch p x >> print (x "name")
        go n = (action n) `catch` ((\_ -> go $ n + 1) :: IOError -> IO ())

fetch :: FilePath -> Context -> IO ()
fetch path ctx = do
  let (ex,symb) = second tail . break (== ':') . unpack $ ctx "name" 
  let req = mkRequest ctx
  withManager $ \manager -> do
    Response _ _ _ src <- http req manager
    src C.$$ sinkFile $ path </> (ex ++ ('-':symb) ++ ".csv")
          
backoff :: Int -> Int
backoff 0 = 0 
backoff x = floor ((1.4 :: Double) ^ (x - 1) * 1000000.0)

googleQString :: Template
googleQString = template "q=${name}&startdate=${date}&output=csv"

mkContext :: Day -> B.ByteString -> Context
mkContext day query = 
  let date = pack . urlEncode . formatTime defaultTimeLocale "%b %d, %Y" $ day
      query' = pack . B.unpack $ query
  in \x -> case x of
             "name" -> query'
             "date" -> date
             _      -> error "context supplied with incorrect parameter"

mkRequest :: Context -> Request m
mkRequest x = def { host = "www.google.com"
                  , path = "/finance/historical"
                  , queryString = encodeUtf8 $ LT.toStrict $ render googleQString x
                  }

getMaxDate :: IConnection conn => conn -> IO Day
getMaxDate conn = do res <- quickQuery' conn "select max(business_date) from daily_price;" []
                     return . fromSql . head . head $ res
