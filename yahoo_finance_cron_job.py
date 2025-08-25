import sys
import os
import json
import pathlib
import time
import logging
import requests
import traceback
from datetime import datetime
from urllib.parse import urlencode
# from selenium import webdriver
# from selenium.webdriver.firefox.service import Service as FirefoxService
# from webdriver_manager.firefox import GeckoDriverManager

from models import formula
from models import stock


afscreener_url = os.environ.get("AF_URL", "")
afscreener_token = os.environ.get("AF_TOKEN", "")


DELAY_TIME_SEC = 10
RETRY_SEND_REQUEST = 5
RETRY_FAILED_DELAY = 80
UPDATE_INTERVAL = 60 * 60 * 24 * 7  # 1 week
BATCH_DB_UPDATE = 10
BATCH_GITHUB_UPDATE = 100


def send_request(url, retry):
    for r in range(retry):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            if res.status_code == 200:
                return 0, res.text
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')
        
        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def send_yahoo_request(url, retry):
    for r in range(retry):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            if res.status_code == 200:
                return 0, res.text
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def send_post_json(url, retry, req_data):
    for r in range(retry):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
            res = requests.post(url, req_data, headers=headers)
            res.raise_for_status()
            if res.status_code == 200:
                return 0, res.text
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')
        time.sleep(RETRY_FAILED_DELAY)

    return -2, "exceed retry cnt"


def get_stock_data(symbol):
    ret, resp = send_request("https://hk.finance.yahoo.com/quote/" + symbol + "?p=" + symbol, RETRY_SEND_REQUEST)
    if ret != 0:
        logging.error('get yahoo data failed')
        exit(-1)

    # logging.info(resp)
    root_app_main = None
    for line in resp.splitlines():
        if "root.App.main" in line:
            temp = line.replace("root.App.main = ", "")
            temp = temp[:len(temp) - 1]
            root_app_main = json.loads(temp)
            break
    if root_app_main is None:
        logging.error('parse yahoo data failed')
        exit(-1)

    return root_app_main


def get_stock_data_v2(symbol, retry):
    ret, resp = send_yahoo_request("https://hk.finance.yahoo.com/quote/" + symbol + "?p=" + symbol, retry)
    if ret != 0:
        logging.error('get yahoo data failed')
        return None

    quoteSummary = None
    for line in resp.splitlines():
        if "/finance/quoteSummary" in line:
            # <script type="application/json" data-sveltekit-fetched data-url="https://query1.finance.yahoo.com/v10/finance/quoteSummary/EW?formatted=true&amp;modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2Cearnings%2CequityPerformance%2CsummaryDetail%2CdefaultKeyStatistics%2CcalendarEvents%2CesgScores%2Cprice%2CpageViews%2CfinancialsTemplate%2CquoteUnadjustedPerformanceOverview&amp;lang=zh-Hant-HK&amp;region=HK&amp;crumb=I1dTi.5r5cw" data-ttl="1">{"status":200,"statusText":"OK","headers":{},"body":"{\"quoteSummary\":{\"result\":[{\"recommendationTrend\":{\"trend\":[{\"period\":\"0m\",\"strongBuy\":7,\"buy\":9,\"hold\":4,\"sell\":0,\"strongSell\":1},{\"period\":\"-1m\",\"strongBuy\":3,\"buy\":11,\"hold\":17,\"sell\":0,\"strongSell\":1},{\"period\":\"-2m\",\"strongBuy\":4,\"buy\":11,\"hold\":16,\"sell\":0,\"strongSell\":1},{\"period\":\"-3m\",\"strongBuy\":6,\"buy\":10,\"hold\":4,\"sell\":0,\"strongSell\":1}],\"maxAge\":86400},\"equityPerformance\":{\"maxAge\":1,\"benchmark\":{\"symbol\":\"^GSPC\",\"shortName\":\"S&P 500\"},\"performanceOverview\":{\"asOfDate\":{\"raw\":1730851200,\"fmt\":\"2024-11-06\"},\"fiveDaysReturn\":{\"raw\":-0.038030025,\"fmt\":\"-3.80%\"},\"oneMonthReturn\":{\"raw\":0.008092821,\"fmt\":\"0.81%\"},\"threeMonthReturn\":{\"raw\":0.087644085,\"fmt\":\"8.76%\"},\"sixMonthReturn\":{\"raw\":-0.22557189,\"fmt\":\"-22.56%\"},\"ytdReturnPct\":{\"raw\":-0.13416398,\"fmt\":\"-13.42%\"},\"oneYearTotalReturn\":{\"raw\":-0.015655331,\"fmt\":\"-1.57%\"},\"twoYearTotalReturn\":{\"raw\":-0.036766913,\"fmt\":\"-3.68%\"},\"threeYearTotalReturn\":{\"raw\":-0.44357356,\"fmt\":\"-44.36%\"},\"fiveYearTotalReturn\":{\"raw\":-0.14779921,\"fmt\":\"-14.78%\"},\"tenYearTotalReturn\":{\"raw\":2.2710156,\"fmt\":\"227.10%\"},\"maxReturn\":{\"raw\":50.950172,\"fmt\":\"5,095.02%\"}},\"performanceOverviewBenchmark\":{\"asOfDate\":{\"raw\":1730851200,\"fmt\":\"2024-11-06\"},\"fiveDaysReturn\":{\"raw\":0.015083405,\"fmt\":\"1.51%\"},\"oneMonthReturn\":{\"raw\":0.026132535,\"fmt\":\"2.61%\"},\"threeMonthReturn\":{\"raw\":0.13786817,\"fmt\":\"13.79%\"},\"sixMonthReturn\":{\"raw\":0.15085833,\"fmt\":\"15.09%\"},\"ytdReturnPct\":{\"raw\":0.23722644,\"fmt\":\"23.72%\"},\"oneYearTotalReturn\":{\"raw\":0.35403848,\"fmt\":\"35.40%\"},\"twoYearTotalReturn\":{\"raw\":0.5651191,\"fmt\":\"56.51%\"},\"threeYearTotalReturn\":{\"raw\":0.25626874,\"fmt\":\"25.63%\"},\"fiveYearTotalReturn\":{\"raw\":0.9193785,\"fmt\":\"91.94%\"},\"tenYearTotalReturn\":{\"raw\":1.9163114,\"fmt\":\"191.63%\"},\"maxReturn\":{\"raw\":2.8726392,\"fmt\":\"287.26%\"}}},\"quoteUnadjustedPerformanceOverview\":{\"maxAge\":1,\"benchmark\":{\"symbol\":\"^GSPC\",\"shortName\":\"S&P 500\"},\"performanceOverview\":{\"asOfDate\":{\"raw\":1730851200,\"fmt\":\"2024-11-06\"},\"fiveDaysReturn\":{\"raw\":-0.038030025,\"fmt\":\"-3.80%\"},\"oneMonthReturn\":{\"raw\":0.008092821,\"fmt\":\"0.81%\"},\"threeMonthReturn\":{\"raw\":0.087644085,\"fmt\":\"8.76%\"},\"sixMonthReturn\":{\"raw\":-0.22557189,\"fmt\":\"-22.56%\"},\"ytdReturnPct\":{\"raw\":-0.13416398,\"fmt\":\"-13.42%\"},\"oneYearTotalReturn\":{\"raw\":-0.015655331,\"fmt\":\"-1.57%\"},\"twoYearTotalReturn\":{\"raw\":-0.036766913,\"fmt\":\"-3.68%\"},\"threeYearTotalReturn\":{\"raw\":-0.44357356,\"fmt\":\"-44.36%\"},\"fiveYearTotalReturn\":{\"raw\":-0.14779921,\"fmt\":\"-14.78%\"},\"tenYearTotalReturn\":{\"raw\":2.2710156,\"fmt\":\"227.10%\"},\"maxReturn\":{\"raw\":50.950172,\"fmt\":\"5,095.02%\"}},\"performanceOverviewBenchmark\":{\"asOfDate\":{\"raw\":1730851200,\"fmt\":\"2024-11-06\"},\"fiveDaysReturn\":{\"raw\":0.015083405,\"fmt\":\"1.51%\"},\"oneMonthReturn\":{\"raw\":0.026132535,\"fmt\":\"2.61%\"},\"threeMonthReturn\":{\"raw\":0.13786817,\"fmt\":\"13.79%\"},\"sixMonthReturn\":{\"raw\":0.15085833,\"fmt\":\"15.09%\"},\"ytdReturnPct\":{\"raw\":0.23722644,\"fmt\":\"23.72%\"},\"oneYearTotalReturn\":{\"raw\":0.35403848,\"fmt\":\"35.40%\"},\"twoYearTotalReturn\":{\"raw\":0.5651191,\"fmt\":\"56.51%\"},\"threeYearTotalReturn\":{\"raw\":0.25626874,\"fmt\":\"25.63%\"},\"fiveYearTotalReturn\":{\"raw\":0.9193785,\"fmt\":\"91.94%\"},\"tenYearTotalReturn\":{\"raw\":1.9163114,\"fmt\":\"191.63%\"},\"maxReturn\":{\"raw\":2.8726392,\"fmt\":\"287.26%\"}}},\"esgScores\":{\"maxAge\":86400,\"totalEsg\":{\"raw\":18.57,\"fmt\":\"18.6\"},\"environmentScore\":{\"raw\":2.86,\"fmt\":\"2.9\"},\"socialScore\":{\"raw\":8.08,\"fmt\":\"8.1\"},\"governanceScore\":{\"raw\":7.63,\"fmt\":\"7.6\"},\"ratingYear\":2023,\"ratingMonth\":9,\"highestControversy\":1.0,\"peerCount\":57,\"esgPerformance\":\"UNDER_PERF\",\"peerGroup\":\"Healthcare\",\"relatedControversy\":[\"Operations Incidents\",\"Business Ethics Incidents\"],\"peerEsgScorePerformance\":{\"min\":11.38,\"avg\":21.72719298245615,\"max\":34.8},\"peerGovernancePerformance\":{\"min\":5.29,\"avg\":7.560714285714284,\"max\":11.03},\"peerSocialPerformance\":{\"min\":5.53,\"avg\":11.985178571428568,\"max\":21.44},\"peerEnvironmentPerformance\":{\"min\":0.01,\"avg\":2.189642857142858,\"max\":5.35},\"peerHighestControversyPerformance\":{\"min\":0.0,\"avg\":1.9473684210526316,\"max\":4.0},\"percentile\":{\"raw\":22.56,\"fmt\":\"23\"},\"environmentPercentile\":null,\"socialPercentile\":null,\"governancePercentile\":null,\"adult\":false,\"alcoholic\":false,\"animalTesting\":true,\"catholic\":false,\"controversialWeapons\":false,\"smallArms\":false,\"furLeather\":false,\"gambling\":false,\"gmo\":false,\"militaryContract\":false,\"nuclear\":false,\"pesticides\":false,\"palmOil\":false,\"coal\":false,\"tobacco\":false},\"defaultKeyStatistics\":{\"maxAge\":1,\"priceHint\":{\"raw\":2,\"fmt\":\"2\",\"longFmt\":\"2\"},\"enterpriseValue\":{\"raw\":38944456704,\"fmt\":\"38.94十億\",\"longFmt\":\"38,944,456,704\"},\"forwardPE\":{\"raw\":26.946938,\"fmt\":\"26.95\"},\"profitMargins\":{\"raw\":0.65859,\"fmt\":\"65.86%\"},\"floatShares\":{\"raw\":596351904,\"fmt\":\"596.35百萬\",\"longFmt\":\"596,351,904\"},\"sharesOutstanding\":{\"raw\":602400000,\"fmt\":\"602.4百萬\",\"longFmt\":\"602,400,000\"},\"sharesShort\":{\"raw\":8220576,\"fmt\":\"8.22百萬\",\"longFmt\":\"8,220,576\"},\"sharesShortPriorMonth\":{\"raw\":11382254,\"fmt\":\"11.38百萬\",\"longFmt\":\"11,382,254\"},\"sharesShortPreviousMonthDate\":{\"raw\":1726185600,\"fmt\":\"2024-09-13\"},\"dateShortInterest\":{\"raw\":1728950400,\"fmt\":\"2024-10-15\"},\"sharesPercentSharesOut\":{\"raw\":0.0136,\"fmt\":\"1.36%\"},\"heldPercentInsiders\":{\"raw\":0.00898,\"fmt\":\"0.90%\"},\"heldPercentInstitutions\":{\"raw\":0.86442,\"fmt\":\"86.44%\"},\"shortRatio\":{\"raw\":1.15,\"fmt\":\"1.15\"},\"shortPercentOfFloat\":{\"raw\":0.0138,\"fmt\":\"1.38%\"},\"beta\":{\"raw\":1.128,\"fmt\":\"1.13\"},\"impliedSharesOutstanding\":{\"raw\":608894976,\"fmt\":\"608.89百萬\",\"longFmt\":\"608,894,976\"},\"morningStarOverallRating\":{},\"morningStarRiskRating\":{},\"category\":null,\"bookValue\":{\"raw\":12.348,\"fmt\":\"12.35\"},\"priceToBook\":{\"raw\":5.346615,\"fmt\":\"5.35\"},\"annualReportExpenseRatio\":{},\"ytdReturn\":{},\"beta3Year\":{},\"totalAssets\":{},\"yield\":{},\"fundFamily\":null,\"fundInceptionDate\":{},\"legalType\":null,\"threeYearAverageReturn\":{},\"fiveYearAverageReturn\":{},\"priceToSalesTrailing12Months\":{},\"lastFiscalYearEnd\":{\"raw\":1703980800,\"fmt\":\"2023-12-31\"},\"nextFiscalYearEnd\":{\"raw\":1735603200,\"fmt\":\"2024-12-31\"},\"mostRecentQuarter\":{\"raw\":1727654400,\"fmt\":\"2024-09-30\"},\"earningsQuarterlyGrowth\":{\"raw\":6.978,\"fmt\":\"697.80%\"},\"revenueQuarterlyGrowth\":{},\"netIncomeToCommon\":{\"raw\":1567299968,\"fmt\":\"1.57十億\",\"longFmt\":\"1,567,299,968\"},\"trailingEps\":{\"raw\":2.36,\"fmt\":\"2.36\"},\"forwardEps\":{\"raw\":2.45,\"fmt\":\"2.45\"},\"pegRatio\":{\"raw\":3.84,\"fmt\":\"3.84\"},\"lastSplitFactor\":\"3:1\",\"lastSplitDate\":{\"raw\":1590969600,\"fmt\":\"2020-06-01\"},\"enterpriseToRevenue\":{\"raw\":6.167,\"fmt\":\"6.17\"},\"enterpriseToEbitda\":{\"raw\":18.918,\"fmt\":\"18.92\"},\"52WeekChange\":{\"raw\":0.006793499,\"fmt\":\"0.68%\"},\"SandP52WeekChange\":{\"raw\":0.31942737,\"fmt\":\"31.94%\"},\"lastDividendValue\":{},\"lastDividendDate\":{},\"lastCapGain\":{},\"annualHoldingsTurnover\":{}},\"summaryProfile\":{\"address1\":\"One Edwards Way\",\"city\":\"Irvine\",\"state\":\"CA\",\"zip\":\"92614\",\"country\":\"United States\",\"phone\":\"949 250 2500\",\"fax\":\"949 250 2525\",\"website\":\"https://www.edwards.com\",\"industry\":\"Medical Devices\",\"industryKey\":\"medical-devices\",\"industryDisp\":\"醫療裝置\",\"sector\":\"Healthcare\",\"sectorKey\":\"healthcare\",\"sectorDisp\":\"醫療保健\",\"longBusinessSummary\":\"Edwards Lifesciences Corporation provides products and technologies for structural heart disease and critical care monitoring in the United States, Europe, Japan, and internationally. It offers transcatheter heart valve replacement products for the minimally invasive replacement of aortic heart valves under the Edwards SAPIEN family of valves system; and transcatheter heart valve repair and replacement products to treat mitral and tricuspid valve diseases under the PASCAL PRECISION and Cardioband names. The company also provides surgical structural heart solutions, such as aortic surgical valve under the INSPIRIS name; INSPIRIS RESILLA aortic valve, which offers RESILIA tissue and VFit technology; KONECT RESILIA, a pre-assembled tissue valves conduit for complex combined procedures; and MITRIS RESILIA valve. In addition, it offers critical care solutions, including hemodynamic monitoring systems to measure a patient's heart function and fluid status in surgical and intensive care settings under the FloTrac, Acumen IQ sensors, ClearSight, Acumen IQ cuffs, and ForeSight names; HemoSphere, a monitoring platform that displays physiological information; and Acumen Hypotension Prediction Index software that alerts clinicians in advance of a patient developing dangerously low blood pressure. The company distributes its products through a direct sales force and independent distributors. Edwards Lifesciences Corporation was founded in 1958 and is headquartered in Irvine, California.\",\"fullTimeEmployees\":19800,\"companyOfficers\":[],\"irWebsite\":\"http://ir.edwards.com/\",\"maxAge\":86400},\"financialsTemplate\":{\"code\":\"N\",\"maxAge\":1},\"summaryDetail\":{\"maxAge\":1,\"priceHint\":{\"raw\":2,\"fmt\":\"2\",\"longFmt\":\"2\"},\"previousClose\":{\"raw\":66.69,\"fmt\":\"66.69\"},\"open\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"dayLow\":{\"raw\":65.82,\"fmt\":\"65.82\"},\"dayHigh\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"regularMarketPreviousClose\":{\"raw\":66.69,\"fmt\":\"66.69\"},\"regularMarketOpen\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"regularMarketDayLow\":{\"raw\":65.82,\"fmt\":\"65.82\"},\"regularMarketDayHigh\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"dividendRate\":{},\"dividendYield\":{},\"exDividendDate\":{},\"payoutRatio\":{\"raw\":0.0,\"fmt\":\"0.00%\"},\"fiveYearAvgDividendYield\":{},\"beta\":{\"raw\":1.128,\"fmt\":\"1.13\"},\"trailingPE\":{\"raw\":27.974577,\"fmt\":\"27.97\"},\"forwardPE\":{\"raw\":26.946938,\"fmt\":\"26.95\"},\"volume\":{\"raw\":1534462,\"fmt\":\"1.53百萬\",\"longFmt\":\"1,534,462\"},\"regularMarketVolume\":{\"raw\":1534462,\"fmt\":\"1.53百萬\",\"longFmt\":\"1,534,462\"},\"averageVolume\":{\"raw\":6191828,\"fmt\":\"6.19百萬\",\"longFmt\":\"6,191,828\"},\"averageVolume10days\":{\"raw\":6173530,\"fmt\":\"6.17百萬\",\"longFmt\":\"6,173,530\"},\"averageDailyVolume10Day\":{\"raw\":6173530,\"fmt\":\"6.17百萬\",\"longFmt\":\"6,173,530\"},\"bid\":{\"raw\":66.36,\"fmt\":\"66.36\"},\"ask\":{\"raw\":66.42,\"fmt\":\"66.42\"},\"bidSize\":{\"raw\":800,\"fmt\":\"800\",\"longFmt\":\"800\"},\"askSize\":{\"raw\":3200,\"fmt\":\"3.2千\",\"longFmt\":\"3,200\"},\"marketCap\":{\"raw\":39770447872,\"fmt\":\"39.77十億\",\"longFmt\":\"39,770,447,872\"},\"yield\":{},\"ytdReturn\":{},\"totalAssets\":{},\"expireDate\":{},\"strikePrice\":{},\"openInterest\":{},\"fiftyTwoWeekLow\":{\"raw\":58.93,\"fmt\":\"58.93\"},\"fiftyTwoWeekHigh\":{\"raw\":96.12,\"fmt\":\"96.12\"},\"priceToSalesTrailing12Months\":{\"raw\":6.2978745,\"fmt\":\"6.30\"},\"fiftyDayAverage\":{\"raw\":67.806,\"fmt\":\"67.81\"},\"twoHundredDayAverage\":{\"raw\":80.38715,\"fmt\":\"80.39\"},\"trailingAnnualDividendRate\":{\"raw\":0.0,\"fmt\":\"0.00\"},\"trailingAnnualDividendYield\":{\"raw\":0.0,\"fmt\":\"0.00%\"},\"navPrice\":{},\"currency\":\"USD\",\"fromCurrency\":null,\"toCurrency\":null,\"lastMarket\":null,\"coinMarketCapLink\":null,\"volume24Hr\":{},\"volumeAllCurrencies\":{},\"circulatingSupply\":{},\"algorithm\":null,\"maxSupply\":{},\"startDate\":{},\"tradeable\":false},\"earnings\":{\"maxAge\":86400,\"earningsChart\":{\"quarterly\":[{\"date\":\"4Q2023\",\"actual\":{\"raw\":0.64,\"fmt\":\"0.64\"},\"estimate\":{\"raw\":0.64,\"fmt\":\"0.64\"}},{\"date\":\"1Q2024\",\"actual\":{\"raw\":0.66,\"fmt\":\"0.66\"},\"estimate\":{\"raw\":0.64,\"fmt\":\"0.64\"}},{\"date\":\"2Q2024\",\"actual\":{\"raw\":0.7,\"fmt\":\"0.70\"},\"estimate\":{\"raw\":0.69,\"fmt\":\"0.69\"}},{\"date\":\"3Q2024\",\"actual\":{\"raw\":0.67,\"fmt\":\"0.67\"},\"estimate\":{\"raw\":0.64,\"fmt\":\"0.64\"}}],\"currentQuarterEstimate\":{\"raw\":0.55,\"fmt\":\"0.55\"},\"currentQuarterEstimateDate\":\"4Q\",\"currentQuarterEstimateYear\":2024,\"earningsDate\":[{\"raw\":1738666740,\"fmt\":\"2025-02-04\"},{\"raw\":1739188800,\"fmt\":\"2025-02-10\"}],\"isEarningsDateEstimate\":true},\"financialsChart\":{\"yearly\":[{\"date\":2020,\"revenue\":{\"raw\":4386300000,\"fmt\":\"4.39十億\",\"longFmt\":\"4,386,300,000\"},\"earnings\":{\"raw\":823400000,\"fmt\":\"823.4百萬\",\"longFmt\":\"823,400,000\"}},{\"date\":2021,\"revenue\":{\"raw\":5232500000,\"fmt\":\"5.23十億\",\"longFmt\":\"5,232,500,000\"},\"earnings\":{\"raw\":1503100000,\"fmt\":\"1.5十億\",\"longFmt\":\"1,503,100,000\"}},{\"date\":2022,\"revenue\":{\"raw\":5382400000,\"fmt\":\"5.38十億\",\"longFmt\":\"5,382,400,000\"},\"earnings\":{\"raw\":1521900000,\"fmt\":\"1.52十億\",\"longFmt\":\"1,521,900,000\"}},{\"date\":2023,\"revenue\":{\"raw\":6004800000,\"fmt\":\"6十億\",\"longFmt\":\"6,004,800,000\"},\"earnings\":{\"raw\":1402400000,\"fmt\":\"1.4十億\",\"longFmt\":\"1,402,400,000\"}}],\"quarterly\":[{\"date\":\"4Q2023\",\"revenue\":{\"raw\":1534100000,\"fmt\":\"1.53十億\",\"longFmt\":\"1,534,100,000\"},\"earnings\":{\"raw\":369900000,\"fmt\":\"369.9百萬\",\"longFmt\":\"369,900,000\"}},{\"date\":\"1Q2024\",\"revenue\":{\"raw\":1598200000,\"fmt\":\"1.6十億\",\"longFmt\":\"1,598,200,000\"},\"earnings\":{\"raw\":351900000,\"fmt\":\"351.9百萬\",\"longFmt\":\"351,900,000\"}},{\"date\":\"2Q2024\",\"revenue\":{\"raw\":1385900000,\"fmt\":\"1.39十億\",\"longFmt\":\"1,385,900,000\"},\"earnings\":{\"raw\":366300000,\"fmt\":\"366.3百萬\",\"longFmt\":\"366,300,000\"}},{\"date\":\"3Q2024\",\"revenue\":{\"raw\":1354400000,\"fmt\":\"1.35十億\",\"longFmt\":\"1,354,400,000\"},\"earnings\":{\"raw\":3070800000,\"fmt\":\"3.07十億\",\"longFmt\":\"3,070,800,000\"}}]},\"financialCurrency\":\"USD\"},\"calendarEvents\":{\"maxAge\":1,\"earnings\":{\"earningsDate\":[{\"raw\":1738666740,\"fmt\":\"2025-02-04\"},{\"raw\":1739188800,\"fmt\":\"2025-02-10\"}],\"earningsCallDate\":[{\"raw\":1729805400,\"fmt\":\"2024-10-24\"}],\"isEarningsDateEstimate\":true,\"earningsAverage\":{\"raw\":0.55,\"fmt\":\"0.55\"},\"earningsLow\":{\"raw\":0.53,\"fmt\":\"0.53\"},\"earningsHigh\":{\"raw\":0.64,\"fmt\":\"0.64\"},\"revenueAverage\":{\"raw\":1363780000,\"fmt\":\"1.36十億\",\"longFmt\":\"1,363,780,000\"},\"revenueLow\":{\"raw\":1335000000,\"fmt\":\"1.33十億\",\"longFmt\":\"1,335,000,000\"},\"revenueHigh\":{\"raw\":1400000000,\"fmt\":\"1.4十億\",\"longFmt\":\"1,400,000,000\"}},\"exDividendDate\":{},\"dividendDate\":{}},\"pageViews\":{\"shortTermTrend\":\"UP\",\"midTermTrend\":\"UP\",\"longTermTrend\":\"UP\",\"maxAge\":1},\"price\":{\"maxAge\":1,\"preMarketChangePercent\":{\"raw\":0.026540698,\"fmt\":\"2.65%\"},\"preMarketChange\":{\"raw\":1.77,\"fmt\":\"1.77\"},\"preMarketTime\":1730903329,\"preMarketPrice\":{\"raw\":68.46,\"fmt\":\"68.46\"},\"preMarketSource\":\"FREE_REALTIME\",\"postMarketChange\":{},\"postMarketPrice\":{},\"regularMarketChangePercent\":{\"raw\":-0.010046571,\"fmt\":\"-1.00%\"},\"regularMarketChange\":{\"raw\":-0.6700058,\"fmt\":\"-0.67\"},\"regularMarketTime\":1730912465,\"priceHint\":{\"raw\":2,\"fmt\":\"2\",\"longFmt\":\"2\"},\"regularMarketPrice\":{\"raw\":66.02,\"fmt\":\"66.02\"},\"regularMarketDayHigh\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"regularMarketDayLow\":{\"raw\":65.82,\"fmt\":\"65.82\"},\"regularMarketVolume\":{\"raw\":1534462,\"fmt\":\"1.53百萬\",\"longFmt\":\"1,534,462.00\"},\"averageDailyVolume10Day\":{\"raw\":6173530,\"fmt\":\"6.17百萬\",\"longFmt\":\"6,173,530\"},\"averageDailyVolume3Month\":{\"raw\":6191828,\"fmt\":\"6.19百萬\",\"longFmt\":\"6,191,828\"},\"regularMarketPreviousClose\":{\"raw\":66.69,\"fmt\":\"66.69\"},\"regularMarketSource\":\"FREE_REALTIME\",\"regularMarketOpen\":{\"raw\":68.27,\"fmt\":\"68.27\"},\"strikePrice\":{},\"openInterest\":{},\"exchange\":\"NYQ\",\"exchangeName\":\"NYSE\",\"exchangeDataDelayedBy\":0,\"marketState\":\"REGULAR\",\"quoteType\":\"EQUITY\",\"symbol\":\"EW\",\"underlyingSymbol\":null,\"shortName\":\"Edwards Lifesciences Corporatio\",\"longName\":\"Edwards Lifesciences Corporation\",\"currency\":\"USD\",\"quoteSourceName\":\"Nasdaq Real Time Price\",\"currencySymbol\":\"$\",\"fromCurrency\":null,\"toCurrency\":null,\"lastMarket\":null,\"volume24Hr\":{},\"volumeAllCurrencies\":{},\"circulatingSupply\":{},\"marketCap\":{\"raw\":39770447872,\"fmt\":\"39.77十億\",\"longFmt\":\"39,770,447,872.00\"}},\"financialData\":{\"maxAge\":86400,\"currentPrice\":{\"raw\":66.02,\"fmt\":\"66.02\"},\"targetHighPrice\":{\"raw\":90.0,\"fmt\":\"90.00\"},\"targetLowPrice\":{\"raw\":63.0,\"fmt\":\"63.00\"},\"targetMeanPrice\":{\"raw\":75.12,\"fmt\":\"75.12\"},\"targetMedianPrice\":{\"raw\":75.0,\"fmt\":\"75.00\"},\"recommendationMean\":{\"raw\":2.5,\"fmt\":\"2.50\"},\"recommendationKey\":\"buy\",\"numberOfAnalystOpinions\":{\"raw\":27,\"fmt\":\"27\",\"longFmt\":\"27\"},\"totalCash\":{\"raw\":1989799936,\"fmt\":\"1.99十億\",\"longFmt\":\"1,989,799,936\"},\"totalCashPerShare\":{\"raw\":3.303,\"fmt\":\"3.3\"},\"ebitda\":{\"raw\":2058599936,\"fmt\":\"2.06十億\",\"longFmt\":\"2,058,599,936\"},\"totalDebt\":{\"raw\":693000000,\"fmt\":\"693百萬\",\"longFmt\":\"693,000,000\"},\"quickRatio\":{},\"currentRatio\":{},\"totalRevenue\":{\"raw\":6314899968,\"fmt\":\"6.31十億\",\"longFmt\":\"6,314,899,968\"},\"debtToEquity\":{\"raw\":9.234,\"fmt\":\"9.23%\"},\"revenuePerShare\":{\"raw\":10.498,\"fmt\":\"10.50\"},\"returnOnAssets\":{},\"returnOnEquity\":{},\"grossProfits\":{},\"freeCashflow\":{},\"operatingCashflow\":{},\"earningsGrowth\":{\"raw\":7.162,\"fmt\":\"716.20%\"},\"revenueGrowth\":{\"raw\":-0.085,\"fmt\":\"-8.50%\"},\"grossMargins\":{\"raw\":0.766,\"fmt\":\"76.60%\"},\"ebitdaMargins\":{\"raw\":0.32599,\"fmt\":\"32.60%\"},\"operatingMargins\":{\"raw\":0.29113,\"fmt\":\"29.11%\"},\"profitMargins\":{\"raw\":0.65859,\"fmt\":\"65.86%\"},\"financialCurrency\":\"USD\"}}],\"error\":null}}"}</script>
            try:
                temp = line[line.index("\">{") + 2:]
                temp = temp[0:temp.index("</script>")]
                respJson = json.loads(temp)
                quoteSummary = json.loads(respJson["body"])
            except Exception as ex:
                logging.error(traceback.format_exc())
                quoteSummary = None

    if quoteSummary is None:
        logging.error('parse yahoo data failed')

    return quoteSummary


"""
def get_stock_data_by_browser(symbol, retry):
    for r in range(retry):
        try:
            driver.get("https://hk.finance.yahoo.com/quote/" + symbol + "?p=" + symbol)
            time.sleep(DELAY_TIME_SEC)
            if "err=404" in driver.current_url:
                logging.warning('auto redirect to ' + driver.current_url + ', skip it')
                return None
            root_app_main = driver.execute_script("return App.main")
            stores = root_app_main['context']['dispatcher']['stores']
            if "QuoteSummaryStore" not in stores:
                if "PageStore" in stores: # yahoo can't find the symbol
                    logging.warning("yahoo can't find the symbol")
                    return None
                else:
                    logging.warning('may occur encrypted data, retry it')
            else:
                return root_app_main
        except Exception as ex:
            logging.error(traceback.format_exc())
            logging.info(f'retry = {r}')

        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)
"""


def update_db(output, api):
    logging.info(f'update to db, output = {output}')
    if len(output["data"]) > 0:
        # update data to server
        param = {
            'code': afscreener_token,
            'api': api
        }
        encoded_args = urlencode(param)
        query_url = afscreener_url + '?' + encoded_args

        ret, resp = send_post_json(query_url, RETRY_SEND_REQUEST, json.dumps(output))
        if ret == 0:
            try:
                resp_json = json.loads(resp)
                if resp_json["ret"] != 0:
                    logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                    sys.exit(1)
            except Exception as ex:
                logging.error(traceback.format_exc())
        else:
            logging.error('send_post_json failed: {ret}'.format(ret=ret))
            sys.exit(1)

    output["data"] = {}


def update_github():
    command = ('cp -r -f ./master/data-output/esgChart/* ./data-output/esgChart' + ' && '
    + 'cp -f ./master/data-output/stock-benford-law.json ./data-output' + ' && '
    + 'git config --global user.name "zmcx16-bot"' + ' && '
    + 'git config --global user.email "zmcx16-bot@zmcx16.moe"' + ' && '
    + 'git reset --soft HEAD~1' + ' && '
    + 'git add ./data-output/\* && git commit -m "updated data"' + ' && '
    + 'git push -f')
    os.system(command)


def get_af_common_data(api, retry):
    for r in range(retry):
        try:
            param = {
                'code': afscreener_token,
                'api': api
            }
            encoded_args = urlencode(param)
            query_url = afscreener_url + '?' + encoded_args

            ret, resp = send_request(query_url, RETRY_SEND_REQUEST)
            if ret == 0:
                try:
                    resp_json = json.loads(resp)
                    if resp_json["ret"] != 0:
                        logging.error('server err = {err}, msg = {msg}'.format(err=resp_json["ret"], msg=resp_json["err_msg"]))
                    else:
                        return resp_json["data"]
                except Exception as ex:
                    logging.error(traceback.format_exc())
            else:
                logging.error('send_request failed: {ret}'.format(ret=ret))

        except Exception as ex:
            logging.error(traceback.format_exc())

        logging.info(f'retry = {r}')
        time.sleep(RETRY_FAILED_DELAY * r)

    sys.exit(1)

"""
def get_quote_summary_store():
    esg_latest = symbol in current_esg_data and now - UPDATE_INTERVAL < current_esg_data[symbol]["last_update_time"]
    recommendation_latest = symbol in current_recommendation_data and now - UPDATE_INTERVAL < \
                            current_recommendation_data[symbol]["last_update_time"]
    eps_latest = symbol in current_eps_data and now - UPDATE_INTERVAL < current_eps_data[symbol]["last_update_time"]
    if esg_latest and recommendation_latest and eps_latest:
        logging.info(f'no need update {symbol} for esg')
        return True

    data = get_stock_data_by_browser(symbol, RETRY_SEND_REQUEST)
    if data is None:
        return True

    stores = data['context']['dispatcher']['stores']
    if "QuoteSummaryStore" not in stores:
        logging.warning('may occur encrypted data, skip going')
        return False

    # parse esg data
    if not esg_latest:
        d = stores["QuoteSummaryStore"]["esgScores"]
        output_esg["data"][symbol] = {
            "socialScore": "-",
            "governanceScore": "-",
            "environmentScore": "-",
            "percentile": "-",
            "totalEsg": "-",
            "last_update_time": int(datetime.now().timestamp()),
        }

        if len(d) > 0:
            if "socialScore" in d and d["socialScore"] and "raw" in d["socialScore"]:
                output_esg["data"][symbol]["socialScore"] = d["socialScore"]["raw"]
            if "governanceScore" in d and d["governanceScore"] and "raw" in d["governanceScore"]:
                output_esg["data"][symbol]["governanceScore"] = d["governanceScore"]["raw"]
            if "environmentScore" in d and d["environmentScore"] and "raw" in d["environmentScore"]:
                output_esg["data"][symbol]["environmentScore"] = d["environmentScore"]["raw"]
            if "percentile" in d and d["percentile"] and "raw" in d["percentile"]:
                output_esg["data"][symbol]["percentile"] = d["percentile"]["raw"]
            if "totalEsg" in d and d["totalEsg"] and "raw" in d["totalEsg"]:
                output_esg["data"][symbol]["totalEsg"] = d["totalEsg"]["raw"]
        else:
            logging.info(f'{symbol} no ESG update')

        if len(output_esg["data"]) >= BATCH_DB_UPDATE:
            update_db(output_esg, 'update-esg-data')

    # parse recommendation data
    if not recommendation_latest:
        output_recommendation["data"][symbol] = {
            "recommendationKey": "-",
            "recommendationMean": "-",
            "last_update_time": int(datetime.now().timestamp()),
        }
        if "financialData" in stores["QuoteSummaryStore"] and len(stores["QuoteSummaryStore"]["financialData"]) > 0:
            d = stores["QuoteSummaryStore"]["financialData"]
            if "recommendationMean" in d and d["recommendationMean"] and "raw" in d["recommendationMean"]:
                output_recommendation["data"][symbol]["recommendationMean"] = d["recommendationMean"]["raw"]
            if "recommendationKey" in d and d["recommendationKey"]:
                output_recommendation["data"][symbol]["recommendationKey"] = d["recommendationKey"]
        else:
            logging.info(f'{symbol} no recommendation update')

        if len(output_recommendation["data"]) >= BATCH_DB_UPDATE:
            update_db(output_recommendation, 'update-recommendation-data')

    # parse earning data
    if not eps_latest:
        output_eps["data"][symbol] = {
            "quarterly": {},
            "last_update_time": int(datetime.now().timestamp()),
        }
        if "earnings" in stores["QuoteSummaryStore"] and \
                "earningsChart" in stores["QuoteSummaryStore"]["earnings"] and \
                "quarterly" in stores["QuoteSummaryStore"]["earnings"]["earningsChart"] and \
                len(stores["QuoteSummaryStore"]["earnings"]["earningsChart"]["quarterly"]) > 0:
            for d in reversed(stores["QuoteSummaryStore"]["earnings"]["earningsChart"]["quarterly"]):
                if "date" in d and d["date"]:
                    estimate = "-"
                    if "estimate" in d and d["estimate"] and "raw" in d["estimate"]:
                        estimate = d["estimate"]["raw"]
                    actual = "-"
                    if "actual" in d and d["actual"] and "raw" in d["actual"]:
                        actual = d["actual"]["raw"]

                    output_eps["data"][symbol]["quarterly"][d["date"]] = {
                        "actual": actual,
                        "estimate": estimate,
                    }
        else:
            logging.info(f'{symbol} no eps data')

        if len(output_eps["data"]) >= BATCH_DB_UPDATE:
            update_db(output_eps, 'update-eps-data')

    return True
"""


def get_yahoo_data():
    esg_latest = symbol in current_esg_data and now - UPDATE_INTERVAL < current_esg_data[symbol]["last_update_time"]
    recommendation_latest = symbol in current_recommendation_data and now - UPDATE_INTERVAL < \
                            current_recommendation_data[symbol]["last_update_time"]
    eps_latest = symbol in current_eps_data and now - UPDATE_INTERVAL < current_eps_data[symbol]["last_update_time"]
    if esg_latest and recommendation_latest and eps_latest:
        logging.info(f'no need update {symbol} for esg')
        return True

    data = get_stock_data_v2(symbol, RETRY_SEND_REQUEST)
    if data is None:
        return True

    esgScores = {}
    financialData = {}
    earnings = {}
    if "quoteSummary" in data and "result" in data["quoteSummary"] and len(data["quoteSummary"]["result"]) > 0:
        for result in data["quoteSummary"]["result"]:
            if "esgScores" in result:
                esgScores = result["esgScores"]
            if "financialData" in result:
                financialData = result["financialData"]
            if "earnings" in result:
                earnings = result["earnings"]

    # parse esg data
    if not esg_latest:
        d = esgScores
        output_esg["data"][symbol] = {
            "socialScore": "-",
            "governanceScore": "-",
            "environmentScore": "-",
            "percentile": "-",
            "totalEsg": "-",
            "last_update_time": int(datetime.now().timestamp()),
        }

        if len(d) > 0:
            if "socialScore" in d and d["socialScore"] and "raw" in d["socialScore"]:
                output_esg["data"][symbol]["socialScore"] = d["socialScore"]["raw"]
            if "governanceScore" in d and d["governanceScore"] and "raw" in d["governanceScore"]:
                output_esg["data"][symbol]["governanceScore"] = d["governanceScore"]["raw"]
            if "environmentScore" in d and d["environmentScore"] and "raw" in d["environmentScore"]:
                output_esg["data"][symbol]["environmentScore"] = d["environmentScore"]["raw"]
            if "percentile" in d and d["percentile"] and "raw" in d["percentile"]:
                output_esg["data"][symbol]["percentile"] = d["percentile"]["raw"]
            if "totalEsg" in d and d["totalEsg"] and "raw" in d["totalEsg"]:
                output_esg["data"][symbol]["totalEsg"] = d["totalEsg"]["raw"]
        else:
            logging.info(f'{symbol} no ESG update')

        if len(output_esg["data"]) >= BATCH_DB_UPDATE:
            update_db(output_esg, 'update-esg-data')

    # parse recommendation data
    if not recommendation_latest:
        output_recommendation["data"][symbol] = {
            "recommendationKey": "-",
            "recommendationMean": "-",
            "last_update_time": int(datetime.now().timestamp()),
        }
        if financialData and len(financialData) > 0:
            d = financialData
            if "recommendationMean" in d and d["recommendationMean"] and "raw" in d["recommendationMean"]:
                output_recommendation["data"][symbol]["recommendationMean"] = d["recommendationMean"]["raw"]
            if "recommendationKey" in d and d["recommendationKey"]:
                output_recommendation["data"][symbol]["recommendationKey"] = d["recommendationKey"]
        else:
            logging.info(f'{symbol} no recommendation update')

        if len(output_recommendation["data"]) >= BATCH_DB_UPDATE:
            update_db(output_recommendation, 'update-recommendation-data')

    # parse earning data
    if not eps_latest:
        output_eps["data"][symbol] = {
            "quarterly": {},
            "last_update_time": int(datetime.now().timestamp()),
        }
        if earnings and \
                "earningsChart" in earnings and \
                "quarterly" in earnings["earningsChart"] and \
                len(earnings["earningsChart"]["quarterly"]) > 0:
            for d in reversed(earnings["earningsChart"]["quarterly"]):
                if "date" in d and d["date"]:
                    estimate = "-"
                    if "estimate" in d and d["estimate"] and "raw" in d["estimate"]:
                        estimate = d["estimate"]["raw"]
                    actual = "-"
                    if "actual" in d and d["actual"] and "raw" in d["actual"]:
                        actual = d["actual"]["raw"]

                    output_eps["data"][symbol]["quarterly"][d["date"]] = {
                        "actual": actual,
                        "estimate": estimate,
                    }
        else:
            logging.info(f'{symbol} no eps data')

        if len(output_eps["data"]) >= BATCH_DB_UPDATE:
            update_db(output_eps, 'update-eps-data')

    return True


def get_esg_chart():
    now = datetime.now().timestamp()
    file_path = esg_chart_folder / (symbol + '.json')
    logging.info(f'[{s_i + 1} / {len(symbol_list)}] get {symbol} esgChart data')

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            current_data = json.loads(f.read())
        if "update_time" in current_data and \
                now - UPDATE_INTERVAL < datetime.strptime(current_data["update_time"], "%Y-%m-%d %H:%M:%S.%f").timestamp():
            logging.info(f'no need update {symbol} for get_esg_chart')
            return True

    ret, resp = send_request("https://query2.finance.yahoo.com/v1/finance/esgChart?symbol=" + symbol, RETRY_SEND_REQUEST)
    if ret != 0:
        logging.error('get yahoo data failed, skip it')
        return True

    data = json.loads(resp)
    output = {'update_time': str(datetime.now()), 'data': {}}
    if 'esgChart' not in data or 'result' not in data['esgChart'] or len(data['esgChart']['result']) == 0 or \
            (len(data['esgChart']['result']) == 1 and data['esgChart']['result'][0] == {}) or \
            data['esgChart']['error'] is not None:
        logging.error(f'get yahoo data failed or no data ({data}), skip it')
        return True

    output['data'] = data
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(output, separators=(',', ':')))

    return True


def get_benford_law(benford_update_count):
    logging.info(f'get {symbol} benford law data')
    if symbol in stock_benford_law_file["data"] and "update_time" in stock_benford_law_file["data"][symbol] and \
            now - UPDATE_INTERVAL < \
            datetime.strptime(stock_benford_law_file["data"][symbol]["update_time"], "%Y-%m-%d %H:%M:%S.%f").timestamp():
        logging.info(f'no need update {symbol} for benford law')
        return True, benford_update_count

    benford_data = stock.calc_stock_benford_probs(symbol)
    if benford_data is None:
        return False, benford_update_count

    # remove key benfordDigitProbs
    if "benfordDigitProbs" in benford_data:
        del benford_data["benfordDigitProbs"]

    output = {'update_time': str(datetime.now()), 'data': benford_data}
    stock_benford_law_file["data"][symbol] = output
    benford_update_count += 1
    with open(stock_benford_law_file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(stock_benford_law_file, separators=(',', ':')))
    return True, benford_update_count


if __name__ == "__main__":

    # options = webdriver.FirefoxOptions()
    # options.headless = True
    # driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)

    logging.basicConfig(level=logging.INFO)

    root = pathlib.Path(__file__).parent.resolve()
    esg_chart_folder = root / "data-output" / "esgChart"
    if not os.path.exists(esg_chart_folder):
        os.makedirs(esg_chart_folder)

    stock_benford_law_file_path = root / "data-output" / "stock-benford-law.json"
    stock_benford_law_file = {}
    if os.path.exists(stock_benford_law_file_path) and os.path.getsize(stock_benford_law_file_path) > 0:
        stock_benford_law_file = json.loads(open(stock_benford_law_file_path, 'r', encoding='utf-8').read())
        if "data" not in stock_benford_law_file:
            stock_benford_law_file["data"] = {}
        if "benfordDigitProbs" not in stock_benford_law_file:
            stock_benford_law_file["benfordDigitProbs"] = formula.Common.benford_digit_probs().tolist()
        if "update_time" not in stock_benford_law_file:
            stock_benford_law_file["update_time"] = str(datetime.now())

    # get stock list
    symbol_list = get_af_common_data('query-stock-list', RETRY_SEND_REQUEST)
    # get seg data
    current_esg_data = get_af_common_data('get-esg-data', RETRY_SEND_REQUEST)
    # get recommendation data
    current_recommendation_data = get_af_common_data('get-recommendation-data', RETRY_SEND_REQUEST)
    # get recommendation data
    current_eps_data = get_af_common_data('get-eps-analysis-data', RETRY_SEND_REQUEST)

    output_esg = {"data": {}}
    output_recommendation = {"data": {}}
    output_eps = {"data": {}}
    benford_update_count = 0
    for s_i in range(len(symbol_list)):
        now = datetime.now().timestamp()
        symbol = symbol_list[s_i]
        logging.info(
            f'[{s_i + 1} / {len(symbol_list)}] get {symbol} data [ESG:({len(output_esg["data"])}) | Recomm:({len(output_recommendation["data"])}) | EPS:({len(output_eps["data"])}) | Benford:({benford_update_count})]')
        if not get_yahoo_data():
            break
        if not get_esg_chart():
            break
        benford_updated, benford_update_count = get_benford_law(benford_update_count)
        if not benford_updated:
            logging.error(f'get {symbol} benford law data failed, skip it')
            continue
        elif (s_i + 1) % BATCH_GITHUB_UPDATE == 0:
            update_github()

    # final update
    update_db(output_esg, 'update-esg-data')
    update_db(output_recommendation, 'update-recommendation-data')
    update_db(output_eps, 'update-eps-data')
    update_github()

    logging.info('all task done')
