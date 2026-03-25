import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from collections import defaultdict
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Stock Trend Trader", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .stApp { background-color: #0a0e17; color: #e8eaf0; }
    section[data-testid="stSidebar"] { background-color: #0f1623; border-right: 1px solid #1e2535; }
    .metric-card { background: linear-gradient(135deg, #141b2d, #1a2340); border: 1px solid #1e2d4a; border-radius: 10px; padding: 14px 18px; margin: 4px 0; }
    .metric-label { font-size: 0.65rem; color: #6b7fa3; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
    .metric-value { font-size: 1.6rem; font-weight: 800; color: #e8eaf0; margin-top: 4px; line-height: 1; }
    .metric-value.positive { color: #00e5b3; }
    .metric-value.negative { color: #ff5555; }
    .calmar-value { font-size: 2.4rem; font-weight: 900; color: #f5a623; }
    .stButton > button { background: linear-gradient(135deg, #e63946, #c1121f); color: white; border: none; border-radius: 8px; font-weight: 700; width: 100%; padding: 12px; font-size: 1rem; }
    .stButton > button:hover { opacity: 0.85; }
    div[data-testid="stExpander"] { background: #0f1623; border-radius: 8px; border: 1px solid #1e2535; }
    .info-banner { background: #0d1e3a; border-left: 4px solid #3a7bd5; padding: 12px 18px; border-radius: 6px; margin-bottom: 18px; font-size: 0.9rem; color: #a8bedf; }
    .section-header { font-size: 1.2rem; font-weight: 700; color: #e8eaf0; margin: 20px 0 10px 0; padding-bottom: 8px; border-bottom: 2px solid #1e2d4a; }
    .filter-on  { color: #00e5b3; font-size: 0.75rem; font-weight: 700; }
    .filter-off { color: #6b7fa3; font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# UNIVERSE — S&P 500 + S&P 400 + Nasdaq extensions
# ~800 quality tickers, all $5+, good liquidity
# ─────────────────────────────────────────────
SP500 = [
    "A","AAL","AAP","AAPL","ABBV","ABC","ABT","ACGL","ACN","ADP","ADSK","AES","AFL","AIG","AIZ",
    "AJG","AKAM","ALB","ALGN","ALK","ALL","ALLE","AMAT","AMCR","AMD","AME","AMGN","AMP","AMT",
    "AMZN","ANET","ANF","AON","AOS","APA","APD","APH","APTV","ARE","ATO","AVB","AVGO","AVY",
    "AWK","AXON","AXP","AYI","AZO","BA","BAC","BAX","BBWI","BBY","BDX","BEN","BF-B","BG",
    "BIIB","BK","BKNG","BKR","BLK","BMY","BR","BRK-B","BSX","BWA","BX","BXP","C","CAG","CAH",
    "CARR","CAT","CB","CBOE","CBRE","CCI","CCL","CDNS","CDW","CE","CEG","CF","CFG","CHD","CHRW",
    "CHTR","CI","CINF","CL","CLX","CMA","CMCSA","CME","CMG","CMI","CMS","CNC","CNP","COF",
    "COO","COP","COST","CPB","CPRT","CRL","CRM","CSCO","CSX","CTAS","CTLT","CTRA","CTSH","CTVA",
    "CVS","CVX","D","DAL","DAY","DD","DE","DFS","DG","DGX","DHI","DHR","DIS","DISH","DLR",
    "DLTR","DOV","DOW","DPZ","DRI","DTE","DUK","DVA","DVN","DXCM","EA","EBAY","ECL","ED","EFX",
    "EIX","EL","EMN","EMR","ENPH","EOG","EPAM","EQIX","EQR","EQT","ES","ESS","ETN","ETR",
    "ETSY","EVRG","EW","EXC","EXPD","EXPE","EXR","F","FANG","FAST","FCX","FDS","FDX","FE",
    "FICO","FIS","FITB","FLT","FMC","FOX","FOXA","FRT","FTNT","FTV","GD","GE","GEHC","GEN",
    "GILD","GIS","GL","GLW","GM","GNRC","GPC","GPN","GPS","GS","GWW","HAL","HAS","HCA","HD",
    "HES","HIG","HII","HLT","HOG","HOLX","HON","HPE","HPQ","HRL","HSIC","HST","HSY","HUM",
    "HWM","IBM","ICE","IDXX","IEX","IFF","ILMN","INCY","INTC","INTU","INVH","IP","IPG","IQV",
    "IR","IRM","ISRG","IT","ITW","IVZ","J","JBHT","JCI","JKHY","JNJ","JNPR","JPM","K","KDP",
    "KEY","KEYS","KHC","KIM","KLAC","KMB","KMI","KMX","KO","KR","L","LDOS","LEN","LH","LHX",
    "LIN","LKQ","LLY","LMT","LNC","LNT","LOW","LRCX","LUV","LVS","LW","LYB","LYV","MA","MAA",
    "MAR","MAS","MCD","MCHP","MCK","MCO","MDLZ","MDT","MET","META","MGM","MHK","MKC","MKTX",
    "MLM","MMC","MMM","MNST","MO","MOH","MOS","MPC","MPWR","MRK","MRNA","MRO","MS","MSCI",
    "MSFT","MSI","MTB","MTD","MU","NEE","NEM","NET","NFLX","NI","NKE","NOC","NOW","NRG","NSC",
    "NTAP","NTRS","NUE","NVDA","NVR","NWS","NWSA","NXPI","O","ODFL","OGN","OKE","OMC","ON",
    "ORCL","ORLY","OTIS","OXY","PAYC","PCAR","PCG","PEAK","PEG","PEP","PFE","PFG","PG","PGR",
    "PH","PHM","PKG","PLD","PM","PNC","PNR","PNW","POOL","PPG","PPL","PRU","PSA","PSX","PTC",
    "PWR","PYPL","QCOM","RCL","REG","REGN","RF","RHI","RJF","RL","RMD","ROK","ROL","ROP",
    "ROST","RSG","RTX","SBAC","SBUX","SCHW","SJM","SLB","SNA","SNPS","SO","SPG","SPGI","SRE",
    "STE","STT","STX","STZ","SWK","SWKS","SYF","SYK","SYY","T","TAP","TDG","TDY","TECH","TEL",
    "TER","TFC","TFX","TGT","TJX","TMO","TMUS","TPR","TRMB","TROW","TRV","TSCO","TSLA","TSN",
    "TT","TTWO","TXN","TYL","UAL","UDR","UHS","ULTA","UNH","UNP","UPS","URI","USB","V","VFC",
    "VICI","VLO","VMC","VRSN","VRTX","VTR","VTRS","VZ","WAB","WAT","WBA","WBD","WDC","WELL",
    "WFC","WHR","WM","WMB","WMT","WRB","WRK","WST","WTW","WY","WYNN","XEL","XOM","XYL",
    "YUM","ZBH","ZBRA","ZION","ZTS",
]

SP400 = [
    "AAN","ACHC","ACM","AGIO","AGO","AGR","AKR","AL","ALKS","ALLE","ALSN","AMKR","AMR","ANDE",
    "ANH","ANIK","ANSS","APOG","AR","ARCB","ARCH","ARE","ARKO","ARL","ARNC","AROC","ARW","ASB",
    "ASGN","ASH","ASO","ASTE","AT","ATI","ATLC","ATN","ATR","AX","AYI","AZZ","BC","BCC","BDC",
    "BDN","BECN","BHB","BHLB","BIG","BMI","BMS","BN","BNL","BOKF","BOOT","BR","BRBR","BRKL",
    "BRKR","BRT","BSX","BTU","BWA","CABO","CACI","CAKE","CAL","CALM","CALX","CAMT","CAR","CARG",
    "CARS","CASY","CATO","CATY","CBT","CDAY","CENE","CENX","CF","CFR","CHE","CHDN","CHEF","CHRD",
    "CHUY","CIR","CIVI","CLF","CLH","CMC","CNMD","CNO","CNX","COHU","COLB","COLM","COMM","COO",
    "COOP","CORT","CPRT","CRC","CRI","CRL","CROX","CRS","CRUS","CSL","CUBE","CVLT","CWST","DAN",
    "DCI","DDS","DINO","DKS","DORM","DRH","DRQ","DUAL","EBC","ENS","ENSG","EPRT","EQH","ERIE",
    "ESAB","ESNT","EXPI","EXTR","FAF","FARO","FBHS","FHB","FHN","FN","FND","FORM","FR","FUL",
    "FULT","GBT","GCI","GFF","GKOS","GLPI","GMS","GPC","GPRO","GRC","GVA","HAE","HAFC","HAYN",
    "HBB","HBI","HBT","HCC","HCI","HCSG","HE","HIBB","HLI","HMN","HNI","HOG","HOMB","HOPE",
    "HOV","HRMY","HROW","HRZN","HTH","HXL","HZO","IBP","ICFI","ICUI","IDA","IDCC","INDB","INSP",
    "ITT","JBSS","JELD","JEF","JOUT","JWN","KAI","KBH","KBR","KFRC","KLIC","KMPR","KNSL","KNTK",
    "KRC","KRG","KTOS","KW","KWR","LAD","LBRT","LCII","LII","LITE","LNC","LSTR","MAA","MANH",
    "MATX","MIDD","MLCO","MMS","MNRO","MOG","MPW","MTDR","MWA","NEO","NEOG","NNN","NSA","NSP",
    "NVT","OGE","OHI","OI","OIS","OLN","ONB","OPCH","ORI","OZK","PAHC","PAR","PATK","PEB",
    "PENN","PII","PINC","PNM","PNR","POST","PRG","PRK","PTEN","PVH","RBC","RDN","RHI","RLJ",
    "ROCK","RPM","RPRX","RRC","SAFE","SAIA","SBH","SCI","SEIC","SF","SFM","SGH","SHAK","SITE",
    "SIX","SKX","SKY","SLG","SLM","SM","SMPL","SNX","SPSC","SPXC","STC","STLD","SUM","SXT",
    "TBK","TCBI","TEN","TGNA","THG","THS","TKR","TNC","TNET","TOL","TPH","TREX","TRIP","TRMK",
    "TRN","TRUP","TSCO","TUP","UAL","UMBF","UMPQ","UNFI","UNIT","UNTY","VFC","VIRT","VMI","VVV",
    "WAL","WEN","WES","WEX","WNS","WSO","WYNN","XPO","YETI","ZURN",
]

NASDAQ_EXT = [
    "ABNB","ACAD","ADBE","ADSK","ADTN","AEIS","AGCO","AGIO","AKAM","ALKS","ALNY","ALRM","AMAG",
    "AMED","AMPH","AMPL","AMRN","AMSC","ANAB","ANSS","AOSL","APAM","APEI","APLS","APPF","APPN",
    "APVO","ARCT","ARDX","ARGX","ARIS","ARWR","ASND","ATAI","ATEC","ATRI","ATSG","AVGO","AVID",
    "AVIR","AVNS","AVTR","AXNX","AXSM","AZEK","AZPN","BAND","BBIO","BCPC","BIDU","BILL","BIOD",
    "BIRD","BJRI","BKNG","BLKB","BLMN","BLUE","BMBL","BMRN","BNTX","BOOT","BRBR","BREW","BRKR",
    "CABO","CALX","CAMT","CDMO","CDNA","CELH","CFLT","CHGG","CHKP","CHRD","CIFR","CIGI","COIN",
    "COLB","COLL","CONN","COOP","CORR","CPNG","CPSI","CRBU","CRDO","CRMT","CRSP","CRWD","CSBR",
    "CSGS","CSIQ","CSOD","CUBE","DDOG","DFIN","DGII","DIOD","DMRC","DNOW","DORM","DXPE","EVER",
    "EVGO","EVLV","EVOP","EVRI","FARO","FCEL","FEIM","FELE","FFBC","FFIN","FGEN","FIVN","FMNB",
    "FNKO","FOLD","FORG","FORR","FROG","FRME","FRPT","FRSH","FTNT","GABC","GDOT","GKOS","GLDD",
    "GLPI","GNLN","GOOD","GOSS","GPMT","GPRE","GRBK","GRFS","GRND","GRPH","GWRE","HALO","HEAR",
    "HIHO","HIPO","HLLY","HOLX","HOOD","HOOK","HSKA","HTBK","HTHT","HVT","HWKN","HYLN","IART",
    "IBCP","IBKR","ICAD","ICON","ICPT","ICUI","IDCC","IDXX","IDYA","IIVI","ILPT","IMAB","IMAX",
    "IMBI","IMCC","IMCR","IMGN","INCY","INDB","INDI","INFI","INFU","INGN","INKT","INLX","INO",
    "INSE","INSG","INSM","INSP","INTC","INTU","INVA","INVE","IOSP","IRBT","IRDM","IRMD","ISBA",
    "ISRG","ITCI","ITGR","ITRI","ITRM","JAMF","JANX","JBLU","JBSS","JFIN","JILL","JJSF","JKS",
    "JOBY","KALA","KALV","KALU","KDNY","KFRC","KIDS","KIRK","KLXE","KMDA","KNDI","KNSA","KORE",
    "KREF","KRMD","KRTX","KRYS","LAKE","LARK","LAUR","LAW","LAZ","LBAI","LBPH","LCID","LCII",
    "LCNB","LCUT","LEGN","LESL","LGND","LGVN","LI","LILA","LILAK","LKFN","LMAT","LMND","LNSR",
    "LNTH","LOCO","LOGI","LOPE","LOVE","LPLA","LPRO","LPSN","LPTX","LQDA","LRCX","LSBK","LSCC",
    "LSTR","LUCD","LULU","LUNA","LYEL","LYTS","MAIA","MARA","MARK","MATW","MBOT","MBUU","MBWM",
    "MCBS","MCFT","MCNB","MCRB","MCRI","MDGL","MDNA","MDRR","MEDS","MEIP","MELI","MEOH","MERC",
    "MESA","MFIN","MGEE","MGNX","MGPI","MICT","MIGI","MIMO","MIND","MINM","MIRM","MIST","MITK",
    "MKSI","MKTX","MLNK","MLSS","MMAT","MMED","MMSI","MNDO","MNKD","MNMD","MNOV","MNRO","MOBI",
    "MOFG","MOGO","MOMO","MORF","MORN","MPLN","MPVD","MPWR","MRCC","MRMD","MRSN","MRTN","MRUS",
    "MSBI","MSEX","MSGE","MSGN","MSTR","MTBC","MTEM","MTEX","MTLS","MTOR","MTRN","MTRX","MTSI",
    "MVBF","MVST","MYMD","MYND","MYPS","MYRG","NABL","NATH","NATI","NAVI","NBHC","NBTB","NCBS",
    "NCMI","NCNO","NDLS","NEO","NEOG","NEON","NEPH","NETE","NEVI","NEWR","NEXT","NFBK","NFLX",
    "NFNT","NGEN","NGMS","NHWK","NICE","NIHD","NKLA","NKTR","NLSN","NLY","NMFC","NMIH","NMRK",
    "NNBR","NNDM","NOVN","NOVT","NPCE","NRDS","NREF","NRIX","NRXP","NSA","NSTG","NTCT","NTGR",
    "NTIC","NTST","NUAN","NUVA","NUVL","NVAX","NVEI","NVGS","NVIV","NVNI","NVNO","NVOS","NVRI",
    "NVRO","NWBI","NWFL","NXGN","NXRT","NXST","NYMT","NYMX","OKTA","OLMA","OLPX","OMAB","OMCL",
    "OMGA","ONCT","ONDAS","ONEW","ONLN","ONMD","ONON","ONPH","OPBK","OPEN","OPGN","OPHC","OPNT",
    "OPRA","OPRT","OPSN","OPTX","ORGO","ORPH","OSCR","OSIS","OSPN","OSTK","OSUR","OTEX","OTMO",
    "OVBC","OVID","OVLY","OYST","PAAS","PACB","PACK","PACW","PALI","PALT","PANL","PARD","PATI",
    "PAYC","PAYS","PBFS","PBH","PBPB","PBYI","PCCA","PCCO","PCOR","PCTI","PCYG","PDCO","PDEX",
    "PDFS","PDLB","PEGY","PERI","PFBC","PFBI","PFIS","PGNY","PHIC","PHIO","PHMD","PHUN","PINE",
    "PINS","PIRS","PKBK","PKOH","PLAB","PLAN","PLBC","PLBY","PLCE","PLUG","PLUS","PLXP","POET",
    "POLA","PODD","POWL","PRAA","PRCT","PRDO","PRET","PREX","PRTK","PRTY","PRVB","PRVC","PSEC",
    "PSHG","PSNL","PSTL","PTCT","PTIX","PTLO","PTMN","PTSI","PULM","PVBC","PWOD","PXLW","PXMD",
    "PYXS","PZZA","QFIN","QMCO","QNST","QUBT","QUIK","QURE","RADI","RADN","RAIN","RAPN","RARE",
    "RAVE","RAYA","RBBN","RBLX","RCKT","RCKY","RCML","RCMT","RCRT","RDHL","RDIB","RDNT","RDVT",
    "RDWR","REAX","RELI","RENN","REPX","RETO","RFIL","RFND","RGCO","RGEN","RGLD","RGLS","RGNX",
    "RIBT","RICK","RICO","RIGL","RILY","RIOT","RLGT","RLJE","RLMD","RMCF","RMNI","RNET","RNLX",
    "RNST","RNWK","ROAD","ROCC","ROCK","ROIV","ROVR","RPAY","RPID","RPTX","RRBI","RRGB","RRTS",
    "RSEM","RSVR","RTLX","RTRX","RUBY","RUMB","RVNC","RVPH","RVSB","RWLK","RXMD","RXRX","RYAM",
    "SAFE","SAFM","SAGE","SAIL","SAMG","SANG","SANM","SASR","SAST","SBCF","SBFG","SBGL","SBNY",
    "SBOW","SBRE","SBSI","SBSW","SCHL","SCHN","SCOR","SCPH","SCPS","SCRM","SCSC","SCVL","SEAL",
    "SEER","SEMA","SENS","SERV","SFBS","SFNC","SGEN","SGHC","SGMO","SGMT","SGRP","SGRY","SGTX",
    "SHBI","SHCA","SHCR","SHFS","SHLX","SHMD","SHOO","SHPW","SIDU","SIEB","SILK","SILV","SILO",
    "SIOX","SIRV","SITM","SJW","SKIL","SKLZ","SLCA","SLDB","SLGL","SLNA","SLNO","SLRC","SLRN",
    "SLRX","SLVM","SMBC","SMFL","SMMT","SMMF","SMSI","SMTC","SMTI","SNBR","SNCE","SNDA","SNEX",
    "SNGX","SNPO","SNPX","SNSE","SNSR","SNST","SNTG","SNUG","SNVS","SODI","SOGP","SOLO","SONM",
    "SONN","SOPA","SOUN","SPFI","SPNE","SPNS","SPOK","SPPL","SPRB","SPRC","SPRO","SPRT","SPSC",
    "SPTN","SPWH","SPXC","SQNS","SQSP","SREV","SRMX","SRNE","SRPT","SRRK","SRTS","SSBI","SSBT",
    "SSBK","SSFN","SSNT","SSRM","SSSS","SSTI","SSYS","STAA","STAF","STBA","STCN","STGW","STIM",
    "STKL","STLA","STLC","STNE","STNG","STOK","STPC","STRL","STRN","STRO","STRR","STRS","STRT",
    "STSA","STXS","SUMO","SUNN","SUPV","SURG","SWAG","SWAV","SWBI","SWIR","SWKH","SYBT","SYBX",
    "SYNH","SYRA","SYTA","TACT","TAIT","TALO","TANH","TAOP","TARA","TBBB","TBBS","TBIO","TBLM",
    "TBLT","TBNK","TBPH","TCBC","TCBK","TCBS","TCDA","TCFC","TCMD","TCRR","TCRX","TDAC","TDIV",
    "TDOC","TDUP","TELA","TELL","TENK","TENX","TERN","TESS","TGLS","TGNA","TGTX","THFF","THGS",
    "THMO","THRY","TIAX","TILS","TIRX","TITN","TLIS","TLGA","TLND","TLPH","TLRY","TMDI","TMDX",
    "TMVW","TNYA","TOEM","TOGI","TOKN","TOLS","TOMZ","TOPS","TOST","TOWR","TPBK","TPCO","TPIC",
    "TPST","TRAK","TRCA","TRCN","TREE","TRES","TRHC","TRIN","TRIP","TRIV","TRMK","TRNS","TRPH",
    "TRPL","TRPX","TRST","TRTX","TRVG","TRVN","TSBK","TSEM","TSHA","TSOI","TSRI","TTCF","TTGT",
    "TTOO","TTSH","TUSK","TUYA","TWST","TXMD","TXNM","TXPH","TXRH","TYME","UBCP","UBFO","UBOH",
    "UBSI","UCBI","UCTT","UDMY","UEIC","ULBI","ULCC","UMBF","UMPQ","UNFI","UNIT","UNTY","UONE",
    "UONEK","UPBD","UPHL","UPST","URBN","URGN","USAP","USAT","USBI","USEG","USFD","USIO","USLM",
    "USPH","UUUU","UVSP","VABK","VALU","VAXX","VCNX","VCSA","VCTR","VCYT","VECO","VEEV","VERI",
    "VERO","VIAV","VICR","VIOT","VIPS","VIRC","VIRI","VISL","VISM","VIST","VITL","VITX","VIVK",
    "VIVO","VLRS","VNET","VNRX","VOXX","VRAY","VRCA","VRDN","VREX","VRGX","VRNA","VRPX","VSAC",
    "VSCO","VSEC","VSTE","VTGN","VTOL","VTRS","VTSI","VTYX","VUZI","VVOS","VXRT","VYGR","VYNT",
    "WABC","WAFD","WAIT","WATT","WCLD","WDAY","WDFC","WEBR","WEGE","WERN","WEYS","WFCF","WFRD",
    "WGBS","WINT","WIRE","WISA","WISH","WISP","WKHS","WKME","WLFC","WMRD","WNEB","WNNR","WOLF",
    "WOOF","WORX","WRBY","WSBC","WSFS","WSTG","WSTL","WTBA","WTFC","WTRG","WULF","WWAC",
]

FULL_UNIVERSE = sorted(set(SP500 + SP400 + NASDAQ_EXT))


# ─────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data(tickers, start_date, end_date):
    try:
        raw = yf.download(list(tickers), start=start_date, end=end_date,
                          auto_adjust=True, progress=False, threads=True)
        if raw.empty:
            return {}
        if len(tickers) == 1:
            raw.columns = pd.MultiIndex.from_product([raw.columns, list(tickers)])
        result = {}
        for col in ["Open","High","Low","Close","Volume"]:
            if col in raw.columns:
                result[col] = raw[col]
        return result
    except Exception as e:
        st.error(f"Data error: {e}")
        return {}

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_etf(ticker, start_date, end_date):
    try:
        df = yf.download(ticker, start=str(start_date), end=str(end_date),
                         auto_adjust=True, progress=False)
        if df.empty:
            return None
        return df["Close"].squeeze()
    except Exception:
        return None


# ─────────────────────────────────────────────
# BACKTEST ENGINE
# strategy_mode: "atr" | "mtp" | "10bar"
# ─────────────────────────────────────────────
def run_backtest(data, tickers, params, filters, initial_capital,
                 strategy_mode="atr", progress_bar=None):
    bp   = params["breakout_period"]
    ap   = params["atr_period"]
    am   = params["atr_mult"]
    ms   = params["min_spacing"]
    ma   = params["max_age"]
    mp   = params["max_pyramid"]
    s2   = params["sma200"]
    s5   = params["sma50"]
    vmp  = params["vol_ma_period"]
    vsm  = params["vol_spike_mult"]
    mav  = params["min_avg_vol"]
    brp  = params["base_risk_pct"]
    mnp  = params["min_price"]
    rev  = params.get("stop_review_bars", 10)

    # Filters
    use_rs          = filters.get("use_rs", False)
    rs_pct          = filters.get("rs_pct", 75)
    use_sma50_rising= filters.get("use_sma50_rising", False)
    use_sma200_pct  = filters.get("use_sma200_pct", False)
    sma200_pct_min  = filters.get("sma200_pct_min", 5.0)
    use_regime      = filters.get("use_regime", False)

    raw   = []
    close = data.get("Close", pd.DataFrame())
    valid = [t for t in tickers if t in close.columns]
    total = len(valid)
    if total == 0:
        return pd.DataFrame()

    # Pre-compute relative strength ranks if needed
    rs_mask = {}
    if use_rs:
        lookback = 126  # ~6 months
        rs_scores = {}
        for t in valid:
            c = close[t].dropna()
            if len(c) > lookback:
                rs_scores[t] = c
        if rs_scores:
            rs_df    = pd.DataFrame(rs_scores)
            rs_ret   = rs_df.pct_change(lookback)
            rs_rank  = rs_ret.rank(axis=1, pct=True)
            threshold = rs_pct / 100.0
            for t in valid:
                if t in rs_rank.columns:
                    rs_mask[t] = rs_rank[t]

    # SPY regime filter
    spy_above_200 = None
    if use_regime:
        spy_raw = fetch_etf("SPY", str(close.index[0].date()), str(close.index[-1].date()))
        if spy_raw is not None:
            spy_sma = spy_raw.rolling(200).mean()
            spy_above_200 = (spy_raw > spy_sma).reindex(close.index).ffill()

    for idx, ticker in enumerate(valid):
        if progress_bar:
            progress_bar.progress((idx+1)/total, text=f"Scanning {ticker}…")
        try:
            c   = data["Close"][ticker].dropna()
            o   = data["Open"][ticker].reindex(c.index).ffill()
            h   = data["High"][ticker].reindex(c.index).ffill()
            lo  = data["Low"][ticker].reindex(c.index).ffill()
            vol = data["Volume"][ticker].reindex(c.index).fillna(0)

            mb = max(bp, ap, s2, vmp, 200) + 10
            if len(c) < mb:
                continue

            tr     = pd.concat([h-lo, (h-c.shift(1)).abs(), (lo-c.shift(1)).abs()], axis=1).max(axis=1)
            atr    = tr.ewm(span=ap, adjust=False).mean()
            sma200 = c.rolling(s2).mean()
            sma50  = c.rolling(s5).mean()
            sma50_prev = sma50.shift(20)
            volma  = vol.rolling(vmp).mean()

            if strategy_mode == "atr":
                cbh  = pd.concat([o, c], axis=1).max(axis=1)
                hb52 = cbh.rolling(bp).max()
            else:
                hb52 = h.rolling(bp).max()

            in_t  = False
            slvl  = entry = hbh = hba = np.nan
            age   = bsa = pyc = bsr = 0
            high_since_review = np.nan

            for j in range(mb, len(c)):
                cv = c.iloc[j]; lv = lo.iloc[j]; hv = h.iloc[j]
                cb = cbh.iloc[j] if strategy_mode == "atr" else hv
                hb = hb52.iloc[j]; av = atr.iloc[j]
                v2 = sma200.iloc[j]; v5 = sma50.iloc[j]
                v5p= sma50_prev.iloc[j]
                vl = vol.iloc[j];  vm = volma.iloc[j]
                dt = c.index[j]

                if pd.isna(hb) or pd.isna(av) or pd.isna(v2):
                    continue

                sd = am * av
                ok = vm >= mav
                vs = vl >= vm * vsm
                a2 = cv > v2
                a5 = cv > v5
                px = cv >= mnp

                # Optional filters
                sma50_rising_ok = True
                if use_sma50_rising and not pd.isna(v5p):
                    sma50_rising_ok = v5 > v5p

                sma200_pct_ok = True
                if use_sma200_pct and not pd.isna(v2) and v2 > 0:
                    sma200_pct_ok = (cv / v2 - 1) * 100 >= sma200_pct_min

                rs_ok = True
                if use_rs and ticker in rs_mask:
                    rs_series = rs_mask[ticker]
                    if dt in rs_series.index:
                        rs_val = rs_series.loc[dt]
                        rs_ok  = not pd.isna(rs_val) and rs_val >= (rs_pct / 100.0)

                regime_ok = True
                if use_regime and spy_above_200 is not None:
                    if dt in spy_above_200.index:
                        regime_ok = bool(spy_above_200.loc[dt])

                if strategy_mode == "atr":
                    esig = (cb >= hb and a2 and a5 and not in_t and vs and ok and px
                            and sma50_rising_ok and sma200_pct_ok and rs_ok and regime_ok)
                else:
                    esig = (hv == hb and a2 and not in_t and vs and ok and px
                            and sma50_rising_ok and sma200_pct_ok and rs_ok and regime_ok)

                asig = in_t and a2 and hv > hba and bsa >= ms and age < ma and vl > vm and pyc < mp
                sh   = in_t and lv < slvl
                te   = in_t and age >= ma

                if esig:
                    in_t  = True
                    entry = cv; slvl = cv - sd; hbh = cb; hba = hv
                    age   = bsa = pyc = bsr = 0
                    high_since_review = hv
                    raw.append({
                        "ticker": ticker, "entry_date": dt,
                        "entry_price": cv, "stop_dist": sd,
                        "rel_vol": vl/vm if vm > 0 else 1.0,
                        "exit_date": None, "exit_price": None,
                        "exit_type": None, "pyramid_adds": 0,
                        "trade_age": 0, "_qty": 0, "_eq": 0.0,
                    })

                elif sh or te:
                    xp = slvl if sh else cv
                    xt = "Stop" if sh else "TimeExit"
                    for s in reversed(raw):
                        if s["ticker"] == ticker and s["exit_date"] is None:
                            s["exit_date"]  = dt; s["exit_price"] = xp
                            s["exit_type"]  = xt; s["pyramid_adds"] = pyc
                            s["trade_age"]  = age; break
                    in_t  = False
                    slvl  = entry = hbh = hba = np.nan
                    age   = bsa = pyc = bsr = 0
                    high_since_review = np.nan

                elif in_t:
                    age += 1; bsa += 1

                    if strategy_mode == "atr":
                        if cb > hbh: hbh = cb
                        ns = hbh - sd
                        if ns > slvl: slvl = ns
                        if asig: hba = hv; bsa = 0; pyc += 1
                        elif hv > hba: hba = hv

                    elif strategy_mode == "mtp":
                        if asig: slvl = cv - sd; hba = hv; bsa = 0; pyc += 1
                        elif hv > hba: hba = hv

                    elif strategy_mode == "10bar":
                        bsr += 1
                        if hv > high_since_review: high_since_review = hv
                        add_fired = False
                        if asig:
                            slvl = cv - sd; hba = hv; bsa = 0; pyc += 1
                            bsr  = 0; high_since_review = hv; add_fired = True
                        if not add_fired and bsr >= rev:
                            if high_since_review > (slvl + sd):
                                ns = high_since_review - sd
                                if ns > slvl: slvl = ns
                            bsr = 0; high_since_review = hv
                        if not asig and hv > hba: hba = hv

        except Exception:
            continue

    raw = [s for s in raw if s["exit_date"] is not None]
    if not raw:
        return pd.DataFrame()

    LEVERAGE = 2.0
    raw.sort(key=lambda x: (x["entry_date"], -x.get("rel_vol", 0)))
    eq = float(initial_capital); bp_ = eq * LEVERAGE
    open_ = {}; trades = []; taken = set()

    all_dates = sorted(set([s["entry_date"] for s in raw] + [s["exit_date"] for s in raw]))
    by_entry  = defaultdict(list)
    by_exit   = defaultdict(list)
    for s in raw:
        by_entry[s["entry_date"]].append(s)
        by_exit[s["exit_date"]].append(s)

    for dt in all_dates:
        for s in by_exit[dt]:
            if id(s) not in taken: continue
            pnl = (s["exit_price"] - s["entry_price"]) * s["_qty"]
            trades.append({
                "ticker":          s["ticker"],
                "entry_date":      s["entry_date"],
                "exit_date":       s["exit_date"],
                "entry_price":     round(s["entry_price"], 4),
                "exit_price":      round(s["exit_price"], 4),
                "qty":             s["_qty"],
                "pnl_pct":         round((s["exit_price"]-s["entry_price"])/s["entry_price"]*100, 4),
                "pnl_dollar":      round(pnl, 4),
                "exit_type":       s["exit_type"],
                "pyramid_adds":    s["pyramid_adds"],
                "trade_age":       s["trade_age"],
                "equity_at_entry": round(s["_eq"], 2),
            })
            open_.pop(s["ticker"], 0)
            eq  = max(eq + pnl, 1.0)
            used = sum(open_.values()); bp_ = max(eq * LEVERAGE - used, 0.0)

        for s in by_entry[dt]:
            sd = s["stop_dist"]
            if sd <= 0 or pd.isna(sd) or s["ticker"] in open_: continue
            qty  = max(1, int(eq * (brp/100) / sd))
            need = qty * s["entry_price"]
            if need > bp_: continue
            taken.add(id(s)); s["_qty"] = qty; s["_eq"] = eq
            open_[s["ticker"]] = need; bp_ -= need

    trades.sort(key=lambda x: x["exit_date"])
    return pd.DataFrame(trades)


# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────
def compute_metrics(df, initial_capital, start_date, end_date):
    if df.empty:
        return {}, pd.Series(dtype=float), pd.Series(dtype=float)
    df = df.copy()
    df["exit_date"]  = pd.to_datetime(df["exit_date"])
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df = df.sort_values("exit_date")
    idx    = pd.date_range(start=start_date, end=end_date, freq="B")
    equity = pd.Series(float(initial_capital), index=idx)
    cum    = 0.0
    for _, t in df.iterrows():
        cum += t["pnl_dollar"]
        equity[equity.index >= t["exit_date"]] = initial_capital + cum
    tr   = (equity.iloc[-1] - equity.iloc[0]) / equity.iloc[0]
    ny   = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    cagr = (1+tr)**(1/ny)-1 if ny > 0 else 0
    dr   = equity.pct_change().dropna()
    sh   = dr.mean()/dr.std()*np.sqrt(252) if dr.std() > 0 else 0
    ds   = dr[dr < 0].std()
    so   = dr.mean()/ds*np.sqrt(252) if ds > 0 else 0
    rm   = equity.cummax(); dd = (equity - rm) / rm; mdd = dd.min()
    cal  = cagr/abs(mdd) if mdd != 0 else 0
    wins = df[df["pnl_dollar"] > 0]; loss = df[df["pnl_dollar"] <= 0]
    wr   = len(wins)/len(df)*100 if len(df) > 0 else 0
    aw   = wins["pnl_pct"].mean() if len(wins) > 0 else 0
    al   = loss["pnl_pct"].mean() if len(loss) > 0 else 0
    gp   = wins["pnl_dollar"].sum() if len(wins) > 0 else 0
    gl   = abs(loss["pnl_dollar"].sum()) if len(loss) > 0 else 1
    pf   = gp/gl if gl > 0 else 0
    exp  = (wr/100*aw) + ((1-wr/100)*al)
    return {
        "Total Return":  f"{tr*100:.2f}%",
        "CAGR":          f"{cagr*100:.2f}%",
        "Sharpe":        f"{sh:.3f}",
        "Sortino":       f"{so:.3f}",
        "Calmar":        f"{cal:.3f}",
        "Max Drawdown":  f"{mdd*100:.2f}%",
        "Total Trades":  len(df),
        "Win Rate":      f"{wr:.2f}%",
        "Avg Win":       f"{aw:.2f}%",
        "Avg Loss":      f"{al:.2f}%",
        "Profit Factor": f"{pf:.2f}",
        "Expectancy":    f"{exp:.2f}%",
        "_cagr": cagr, "_sharpe": sh, "_calmar": cal, "_maxdd": mdd, "_pf": pf,
    }, equity, dd


# ─────────────────────────────────────────────
# BENCHMARKS
# ─────────────────────────────────────────────
def compute_dca(ticker, initial_capital, monthly, start_date, end_date):
    prices = fetch_etf(ticker, str(start_date), str(end_date))
    if prices is None: return None
    idx    = pd.date_range(start=start_date, end=end_date, freq="B")
    prices = prices.reindex(idx).ffill().bfill()
    if prices.isna().all() or prices.iloc[0] == 0: return None
    shares    = initial_capital / prices.iloc[0]
    eq        = pd.Series(index=idx, dtype=float)
    eq.iloc[0]= shares * prices.iloc[0]
    last_m    = prices.index[0].month
    for i, dt in enumerate(prices.index[1:], 1):
        if dt.month != last_m:
            shares += monthly / prices.iloc[i]; last_m = dt.month
        eq.iloc[i] = shares * prices.iloc[i]
    return eq

def add_monthly(equity, monthly):
    if monthly <= 0: return equity
    eq = equity.copy().astype(float); last_m = eq.index[0].month; cum = 0.0
    for i, dt in enumerate(eq.index[1:], 1):
        if dt.month != last_m: cum += monthly; last_m = dt.month
        eq.iloc[i] += cum
    return eq

def calmar_of_etf(ticker, start_date, end_date):
    prices = fetch_etf(ticker, str(start_date), str(end_date))
    if prices is None: return None
    dr  = prices.pct_change().dropna()
    ny  = (prices.index[-1] - prices.index[0]).days / 365.25
    tr  = (prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0]
    cagr= (1+tr)**(1/ny)-1 if ny > 0 else 0
    rm  = prices.cummax(); dd = (prices - rm) / rm; mdd = dd.min()
    return cagr / abs(mdd) if mdd != 0 else 0


# ─────────────────────────────────────────────
# OPTIMIZER
# ─────────────────────────────────────────────
def suggest(space, past, n_start=10):
    def uniform():
        p = {}
        for k, (lo, hi, step, typ) in space.items():
            if typ == "int":
                p[k] = int(np.random.choice(range(lo, hi+1, step)))
            else:
                n = int(round((hi-lo)/step))
                p[k] = round(lo + np.random.randint(0, n+1)*step, 4)
        return p
    if len(past) < n_start: return uniform()
    best = max(past, key=lambda x: x["score"])["params"]
    p = {}
    for k, (lo, hi, step, typ) in space.items():
        if typ == "int":
            noise = int(np.random.choice([-2,-1,0,0,1,2])) * step
            val   = int(np.clip(best[k]+noise, lo, hi))
            p[k]  = min(range(lo, hi+1, step), key=lambda x: abs(x-val))
        else:
            val   = round(np.clip(best[k] + np.random.uniform(-(hi-lo)*0.15, (hi-lo)*0.15), lo, hi), 4)
            n     = int(round((val-lo)/step))
            p[k]  = round(lo + n*step, 4)
    return p


# ─────────────────────────────────────────────
# CHARTS
# ─────────────────────────────────────────────
COLORS = {"atr":"#00e5b3","mtp":"#4a90d9","10bar":"#f5a623","qqq":"#a855f7","spy":"#6b7fa3"}

def chart_comparison(results, qqq=None, spy=None, monthly=0):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72,0.28], vertical_spacing=0.03)
    names = {"atr":"S1: ATR Trailing","mtp":"S2: MTP Static","10bar":"S3: 10-Bar Static"}
    for mode, (equity, dd) in results.items():
        strat = add_monthly(equity, monthly)
        fig.add_trace(go.Scatter(x=strat.index, y=strat.values,
            line=dict(color=COLORS[mode], width=2.5), name=names[mode]), row=1, col=1)
        fig.add_trace(go.Scatter(x=dd.index, y=dd.values*100,
            line=dict(color=COLORS[mode], width=1, dash="dot"),
            name=f"{names[mode]} DD", showlegend=False), row=2, col=1)
    if qqq is not None:
        fig.add_trace(go.Scatter(x=qqq.index, y=qqq.values,
            line=dict(color=COLORS["qqq"], width=1.5, dash="dash"), name="QQQ DCA"), row=1, col=1)
    if spy is not None:
        fig.add_trace(go.Scatter(x=spy.index, y=spy.values,
            line=dict(color=COLORS["spy"], width=1.5, dash="dot"), name="SPY DCA"), row=1, col=1)
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=480,
        showlegend=True, legend=dict(bgcolor="rgba(10,14,23,0.9)", bordercolor="#1e2d4a", borderwidth=1, x=0.01, y=0.99))
    fig.update_xaxes(gridcolor="#141b2d"); fig.update_yaxes(gridcolor="#141b2d")
    fig.update_yaxes(title_text="Portfolio ($)", row=1, col=1)
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
    return fig

def chart_annual(equity, color, name):
    ann    = equity.resample("YE").last().pct_change().dropna() * 100
    colors = [color if v >= 0 else "#ff5555" for v in ann.values]
    fig    = go.Figure(go.Bar(x=ann.index.year, y=ann.values, marker_color=colors, name=name))
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=280,
        xaxis_title="Year", yaxis_title="Return (%)", title=name, title_font=dict(color=color))
    fig.update_xaxes(gridcolor="#141b2d")
    fig.update_yaxes(gridcolor="#141b2d", zeroline=True, zerolinecolor="#2a3550")
    return fig

def chart_dist(df, color, name):
    if df.empty: return go.Figure()
    fig = px.histogram(df, x="pnl_pct", nbins=60, color_discrete_sequence=[color],
                       labels={"pnl_pct":"Trade Return (%)"}, title=name)
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=30,b=0), height=280, title_font=dict(color=color))
    fig.update_xaxes(gridcolor="#141b2d"); fig.update_yaxes(gridcolor="#141b2d")
    return fig

def mc(label, value, pos_good=True, color=None, big=False):
    neg = isinstance(value, str) and value.startswith("-")
    if color:
        cls = "calmar-value" if big else "metric-value"
        return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="{cls}" style="color:{color}">{value}</div></div>'
    css = ("negative" if pos_good else "positive") if neg else ("positive" if pos_good else "negative")
    cls = "calmar-value" if big else f"metric-value {css}"
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="{cls}">{value}</div></div>'


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")

    with st.expander("📊 Universe & Data", expanded=True):
        universe_choice = st.selectbox("Universe", [
            "Full (S&P500 + S&P400 + Nasdaq)",
            "S&P 500 only",
            "S&P 500 + S&P 400",
            "Custom",
        ])
        custom_tickers = []
        if universe_choice == "Custom":
            raw_input = st.text_area("Tickers (comma-separated)", "AAPL, MSFT, NVDA")
            custom_tickers = [t.strip().upper() for t in raw_input.split(",") if t.strip()]

        c1, c2 = st.columns(2)
        with c1: start_date = st.date_input("Start", datetime(2005, 1, 1))
        with c2: end_date   = st.date_input("End",   datetime.today())
        initial_capital      = st.number_input("Capital ($)",     min_value=1000, value=10000, step=1000)
        monthly_contribution = st.number_input("Monthly Add ($)", min_value=0,    value=500,   step=100)

    with st.expander("🔀 Strategies", expanded=True):
        run_s1   = st.toggle("S1 — ATR Trailing Stop",  value=True)
        run_s2   = st.toggle("S2 — MTP Static Stop",    value=True)
        run_s3   = st.toggle("S3 — 10-Bar Static Stop", value=True)
        show_qqq = st.toggle("QQQ benchmark",            value=True)
        show_spy = st.toggle("SPY benchmark",            value=True)

    with st.expander("🎯 Strategy Parameters", expanded=True):
        breakout_period  = st.number_input("52W Lookback",           10,  504,  252)
        atr_period       = st.number_input("ATR Period",             1,   500,  293)
        atr_mult         = st.number_input("ATR Multiplier",         0.1, 20.0, 7.25, step=0.05)
        max_pyramid      = st.number_input("Max Pyramid Adds",       0,   20,   5)
        min_spacing      = st.number_input("Min Bars Between Adds",  1,   100,  38)
        max_age          = st.number_input("Max Trade Age (bars)",   10,  2000, 584)
        base_risk_pct    = st.number_input("Risk % per Trade",       0.1, 10.0, 2.0, step=0.25)
        stop_review_bars = st.number_input("Stop Review Bars (S3)",  1,   50,   10)

    with st.expander("📈 Moving Averages", expanded=True):
        sma200 = st.number_input("Trend SMA",   1, 500, 200)
        sma50  = st.number_input("Context SMA", 1, 300, 50)

    with st.expander("🔊 Volume"):
        vol_ma_period  = st.number_input("Volume MA Period",    5,  100,        20)
        vol_spike_mult = st.number_input("Vol Spike Mult",      1.0, 5.0,       1.5, step=0.1)
        min_avg_vol    = st.number_input("Min Avg Volume",      0,   10_000_000, 1_000_000, step=100_000)

    with st.expander("🔍 Entry Quality Filters", expanded=True):
        st.caption("Each filter narrows entries to only the highest quality setups.")
        min_price = st.number_input("Min Price ($)", 0.0, 500.0, 5.0, step=0.5)

        st.markdown("---")
        use_sma50_rising = st.toggle("50 SMA must be rising", value=False,
            help="50 SMA today must be higher than 20 bars ago — confirms uptrend momentum")
        
        use_sma200_pct = st.toggle("Price X% above 200 SMA", value=False,
            help="Requires price to be meaningfully above the 200 SMA, not just barely")
        sma200_pct_min = 5.0
        if use_sma200_pct:
            sma200_pct_min = st.slider("Min % above 200 SMA", 1.0, 20.0, 5.0, step=0.5)

        use_rs = st.toggle("Relative Strength filter", value=False,
            help="Only enter stocks in the top X% by 6-month return vs the rest of the universe")
        rs_pct = 75
        if use_rs:
            rs_pct = st.slider("Top percentile (higher = stricter)", 50, 95, 75, step=5,
                help="75 = only top 25% of stocks by 6-month momentum")

        use_regime = st.toggle("Market regime filter (SPY 200 SMA)", value=False,
            help="Block all new entries when SPY is below its 200 SMA")

    st.markdown("---")
    run_btn = st.button("🚀 Run Comparison")


# ─────────────────────────────────────────────
# UNIVERSE SELECTION
# ─────────────────────────────────────────────
def get_tickers(choice, custom):
    if choice == "Custom":              return custom
    if choice == "S&P 500 only":        return SP500
    if choice == "S&P 500 + S&P 400":  return list(dict.fromkeys(SP500 + SP400))
    return FULL_UNIVERSE  # Full


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
st.markdown("# 📈 Stock Trend Trader")
st.markdown("*52W Breakout · ATR Trailing · MTP Static · 10-Bar Static · vs QQQ & SPY*")
st.markdown("---")

tab1, tab2 = st.tabs(["📊 Strategy Comparison", "🔬 Optimization"])


# ══════════════════════════════════════════
# TAB 1
# ══════════════════════════════════════════
with tab1:
    if not run_btn:
        tickers_preview = get_tickers(universe_choice, custom_tickers)
        st.markdown(f"""<div class="info-banner">
        Universe: <strong>{len(tickers_preview)} tickers</strong> ({universe_choice}) |
        Data from <strong>Yahoo Finance</strong> | Click <strong>Run Comparison</strong> to start.<br>
        All active strategies run with identical parameters for a fair comparison.
        </div>""", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("""**S1 — ATR Trailing** 🟢
Stop ratchets up every bar from highest candle body high since entry. Most responsive.""")
        with c2:
            st.markdown("""**S2 — MTP Static** 🔵
Stop frozen between adds. Only resets to close − ATR when a pyramid add fires.""")
        with c3:
            st.markdown("""**S3 — 10-Bar Static** 🟠
Stop reviews every N bars. Only updates if new high made in that window. Most stable.""")

        st.markdown('<div class="section-header">🎛 Active Filters</div>', unsafe_allow_html=True)
        fc1, fc2, fc3, fc4, fc5 = st.columns(5)
        with fc1: st.markdown(f'<div class="{"filter-on" if use_sma50_rising else "filter-off"}">{"✅" if use_sma50_rising else "⬜"} 50 SMA Rising</div>', unsafe_allow_html=True)
        with fc2: st.markdown(f'<div class="{"filter-on" if use_sma200_pct else "filter-off"}">{"✅" if use_sma200_pct else "⬜"} {sma200_pct_min:.0f}% above 200 SMA</div>', unsafe_allow_html=True)
        with fc3: st.markdown(f'<div class="{"filter-on" if use_rs else "filter-off"}">{"✅" if use_rs else "⬜"} RS top {100-rs_pct}%</div>', unsafe_allow_html=True)
        with fc4: st.markdown(f'<div class="{"filter-on" if use_regime else "filter-off"}">{"✅" if use_regime else "⬜"} SPY Regime</div>', unsafe_allow_html=True)
        with fc5: st.markdown(f'<div class="filter-on">✅ Min ${min_price:.0f}</div>', unsafe_allow_html=True)

    else:
        active = []
        if run_s1: active.append("atr")
        if run_s2: active.append("mtp")
        if run_s3: active.append("10bar")

        if not active:
            st.error("Select at least one strategy in the sidebar.")
            st.stop()

        tickers = get_tickers(universe_choice, custom_tickers)
        if not tickers:
            st.error("No tickers selected.")
            st.stop()

        params = {
            "breakout_period": breakout_period, "atr_period": atr_period,
            "atr_mult": atr_mult, "min_spacing": min_spacing,
            "max_age": max_age, "max_pyramid": max_pyramid,
            "sma200": sma200, "sma50": sma50,
            "vol_ma_period": vol_ma_period, "vol_spike_mult": vol_spike_mult,
            "min_avg_vol": min_avg_vol, "base_risk_pct": base_risk_pct,
            "min_price": min_price, "stop_review_bars": stop_review_bars,
        }
        filters = {
            "use_rs": use_rs, "rs_pct": rs_pct,
            "use_sma50_rising": use_sma50_rising,
            "use_sma200_pct": use_sma200_pct, "sma200_pct_min": sma200_pct_min,
            "use_regime": use_regime,
        }

        st.info(f"Universe: **{len(tickers)} tickers** | Strategies: **{len(active)}** | {start_date} → {end_date}")

        with st.spinner(f"Downloading data for {len(tickers)} tickers from Yahoo Finance…"):
            data = fetch_data(tuple(tickers), str(start_date), str(end_date))

        if not data:
            st.error("No data returned from Yahoo Finance.")
            st.stop()

        mode_names  = {"atr":"S1: ATR Trailing","mtp":"S2: MTP Static","10bar":"S3: 10-Bar Static"}
        mode_colors = {"atr":COLORS["atr"],"mtp":COLORS["mtp"],"10bar":COLORS["10bar"]}

        all_results = {}; all_metrics = {}; all_trades = {}

        for mode in active:
            prog   = st.progress(0, text=f"Running {mode_names[mode]}…")
            trades = run_backtest(data, tickers, params, filters, initial_capital, mode, prog)
            prog.empty()
            if trades.empty:
                st.warning(f"{mode_names[mode]}: No trades generated.")
                continue
            metrics, equity, drawdown = compute_metrics(trades, initial_capital, start_date, end_date)
            all_results[mode] = (equity, drawdown)
            all_metrics[mode] = metrics
            all_trades[mode]  = trades

        if not all_results:
            st.warning("No results for any strategy.")
            st.stop()

        qqq_eq = spy_eq = None
        if show_qqq:
            with st.spinner("Loading QQQ…"):
                qqq_eq = compute_dca("QQQ", initial_capital, monthly_contribution, start_date, end_date)
        if show_spy:
            with st.spinner("Loading SPY…"):
                spy_eq = compute_dca("SPY", initial_capital, monthly_contribution, start_date, end_date)

        # ── CALMAR SPOTLIGHT ──
        st.markdown('<div class="section-header">🏆 Calmar Ratio — Primary Metric</div>', unsafe_allow_html=True)
        st.caption("Calmar = CAGR ÷ Max Drawdown. Higher is better. This is the most important metric for a trend following strategy.")

        calmar_cols = list(all_results.keys())
        calmar_display = st.columns(len(calmar_cols) + (2 if (show_qqq or show_spy) else 0))

        col_idx = 0
        for mode in calmar_cols:
            m = all_metrics[mode]
            with calmar_display[col_idx]:
                st.markdown(mc(f"{mode_names[mode]} Calmar", m["Calmar"],
                               color=mode_colors[mode], big=True), unsafe_allow_html=True)
                st.markdown(mc("CAGR", m["CAGR"]), unsafe_allow_html=True)
                st.markdown(mc("Max Drawdown", m["Max Drawdown"], False), unsafe_allow_html=True)
            col_idx += 1

        if show_qqq:
            qqq_cal = calmar_of_etf("QQQ", str(start_date), str(end_date))
            if qqq_cal is not None:
                with calmar_display[col_idx]:
                    st.markdown(mc("QQQ Buy & Hold Calmar", f"{qqq_cal:.3f}",
                                   color=COLORS["qqq"], big=True), unsafe_allow_html=True)
                col_idx += 1

        if show_spy:
            spy_cal = calmar_of_etf("SPY", str(start_date), str(end_date))
            if spy_cal is not None:
                with calmar_display[col_idx]:
                    st.markdown(mc("SPY Buy & Hold Calmar", f"{spy_cal:.3f}",
                                   color=COLORS["spy"], big=True), unsafe_allow_html=True)

        # ── Equity curve ──
        st.markdown('<div class="section-header">📈 Equity Curve</div>', unsafe_allow_html=True)
        st.plotly_chart(chart_comparison(all_results, qqq_eq, spy_eq, monthly_contribution), use_container_width=True)

        n_months = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 30.44
        total_in = initial_capital + monthly_contribution * n_months
        st.caption(f"Total contributed: **${total_in:,.0f}** (${initial_capital:,} initial + ${monthly_contribution:,}/mo)")

        # ── Full metrics ──
        st.markdown('<div class="section-header">📊 Full Performance Metrics</div>', unsafe_allow_html=True)
        metric_keys = ["CAGR","Sharpe","Sortino","Calmar","Max Drawdown","Total Trades",
                       "Win Rate","Avg Win","Avg Loss","Profit Factor","Expectancy"]
        pos_good    = [True,True,True,True,False,True,True,True,False,True,True]
        cols = st.columns(len(all_results))
        for i, (mode, metrics) in enumerate(all_metrics.items()):
            with cols[i]:
                st.markdown(f'<div style="color:{mode_colors[mode]};font-weight:700;font-size:1rem;margin-bottom:8px">{mode_names[mode]}</div>', unsafe_allow_html=True)
                for mk, pg in zip(metric_keys, pos_good):
                    st.markdown(mc(mk, metrics[mk], pg), unsafe_allow_html=True)

        # ── Annual returns ──
        st.markdown('<div class="section-header">📅 Annual Returns</div>', unsafe_allow_html=True)
        cols = st.columns(len(all_results))
        for i, (mode, (equity, _)) in enumerate(all_results.items()):
            with cols[i]:
                st.plotly_chart(chart_annual(equity, mode_colors[mode], mode_names[mode]), use_container_width=True)

        # ── Distribution ──
        st.markdown('<div class="section-header">📦 Trade Distribution</div>', unsafe_allow_html=True)
        cols = st.columns(len(all_results))
        for i, (mode, trades) in enumerate(all_trades.items()):
            with cols[i]:
                st.plotly_chart(chart_dist(trades, mode_colors[mode], mode_names[mode]), use_container_width=True)

        # ── Trade logs ──
        st.markdown('<div class="section-header">📋 Trade Logs</div>', unsafe_allow_html=True)
        trade_tabs = st.tabs([mode_names[m] for m in all_trades.keys()])
        for tab, (mode, trades) in zip(trade_tabs, all_trades.items()):
            with tab:
                dcols = ["ticker","entry_date","exit_date","entry_price","exit_price",
                         "pnl_pct","pnl_dollar","exit_type","pyramid_adds","trade_age","equity_at_entry"]
                ddf = trades[dcols].copy()
                for c in ["pnl_pct","pnl_dollar","entry_price","exit_price"]:
                    ddf[c] = ddf[c].round(2)
                st.dataframe(ddf, use_container_width=True, height=350)
                st.download_button(f"⬇️ Download {mode_names[mode]}",
                    ddf.to_csv(index=False), f"trades_{mode}.csv", "text/csv")


# ══════════════════════════════════════════
# TAB 2 — OPTIMIZATION
# ══════════════════════════════════════════
with tab2:
    st.markdown("### 🔬 Parameter Optimization")
    st.markdown("""<div class="info-banner">
    Optimizes a single strategy. Target <strong>Calmar</strong> for the best risk-adjusted results.
    Run multiple times and take the median of the best results.
    </div>""", unsafe_allow_html=True)

    oc1, oc2, oc3 = st.columns(3)
    with oc1: opt_metric   = st.selectbox("Optimize For", ["Calmar","Sharpe","CAGR","Profit Factor"])
    with oc2: opt_strategy = st.selectbox("Strategy", ["S1: ATR Trailing","S2: MTP Static","S3: 10-Bar Static"])
    with oc3: n_trials     = st.number_input("Trials", 5, 200, 40, step=5)

    opt_universe = st.selectbox("Universe", [
        "Full (S&P500 + S&P400 + Nasdaq)",
        "S&P 500 only",
        "S&P 500 + S&P 400",
        "Custom",
    ], key="ou")
    custom_opt = []
    if opt_universe == "Custom":
        ci = st.text_area("Tickers", "AAPL, MSFT, NVDA", key="ct")
        custom_opt = [t.strip().upper() for t in ci.split(",") if t.strip()]

    st.markdown("#### Search Ranges")
    sr1, sr2 = st.columns(2)
    with sr1:
        bp_r = st.slider("52W Lookback",         50,   504,  (150, 350), step=10)
        ap_r = st.slider("ATR Period",            10,   400,  (150, 400), step=5)
        am_r = st.slider("ATR Multiplier",        2.0,  15.0, (5.0, 10.0), step=0.5)
        mp_r = st.slider("Max Pyramid Adds",      0,    15,   (2, 8),    step=1)
        ms_r = st.slider("Min Bars Between Adds", 5,    80,   (20, 60),  step=1)
        ma_r = st.slider("Max Trade Age",         100,  2000, (400, 800), step=50)
    with sr2:
        s2_r = st.slider("Trend SMA",             100,  300,  (150, 250), step=10)
        s5_r = st.slider("Context SMA",           20,   100,  (40, 70),  step=5)
        vs_r = st.slider("Vol Spike Mult",         1.0,  4.0,  (1.2, 2.5), step=0.1)
        br_r = st.slider("Base Risk %",            0.5,  5.0,  (1.0, 4.0), step=0.25)

    oc4, oc5, oc6 = st.columns(3)
    with oc4: opt_start = st.date_input("Start", datetime(2005, 1, 1), key="os")
    with oc5: opt_end   = st.date_input("End",   datetime.today(),     key="oe")
    with oc6: opt_cap   = st.number_input("Capital ($)", 1000, 10_000_000, 10000, step=1000, key="oc")

    run_opt = st.button("🚀 Run Optimization")

    if run_opt:
        opt_mode_map = {"S1: ATR Trailing":"atr","S2: MTP Static":"mtp","S3: 10-Bar Static":"10bar"}
        opt_mode     = opt_mode_map[opt_strategy]
        opt_tickers  = get_tickers(opt_universe, custom_opt)

        if not opt_tickers:
            st.error("No tickers."); st.stop()

        score_map = {"Calmar":"_calmar","Sharpe":"_sharpe","CAGR":"_cagr","Profit Factor":"_pf"}
        sort_map  = {"Calmar":"Calmar","Sharpe":"Sharpe","CAGR":"CAGR%","Profit Factor":"Profit Factor"}
        skey = score_map[opt_metric]
        scol = sort_map[opt_metric]

        space = {
            "breakout_period": (bp_r[0], bp_r[1], 10,   "int"),
            "atr_period":      (ap_r[0], ap_r[1], 5,    "int"),
            "atr_mult":        (am_r[0], am_r[1], 0.5,  "float"),
            "max_pyramid":     (mp_r[0], mp_r[1], 1,    "int"),
            "min_spacing":     (ms_r[0], ms_r[1], 1,    "int"),
            "max_age":         (ma_r[0], ma_r[1], 50,   "int"),
            "sma200":          (s2_r[0], s2_r[1], 10,   "int"),
            "sma50":           (s5_r[0], s5_r[1], 5,    "int"),
            "vol_spike_mult":  (vs_r[0], vs_r[1], 0.1,  "float"),
            "base_risk_pct":   (br_r[0], br_r[1], 0.25, "float"),
        }
        fixed = {
            "vol_ma_period": vol_ma_period, "min_avg_vol": min_avg_vol,
            "min_price": min_price, "stop_review_bars": stop_review_bars,
        }
        opt_filters = {
            "use_rs": use_rs, "rs_pct": rs_pct,
            "use_sma50_rising": use_sma50_rising,
            "use_sma200_pct": use_sma200_pct, "sma200_pct_min": sma200_pct_min,
            "use_regime": use_regime,
        }

        with st.spinner(f"Downloading {len(opt_tickers)} tickers…"):
            opt_data = fetch_data(tuple(opt_tickers), str(opt_start), str(opt_end))

        if not opt_data:
            st.error("No data."); st.stop()

        results = []; past = []; best_s = -np.inf; best_t = None
        tbl  = st.empty()
        prog = st.progress(0, text="Starting…")

        for n in range(1, n_trials+1):
            prog.progress(n/n_trials, text=f"Trial {n}/{n_trials} | Best {opt_metric}: {best_s:.4f}")
            tp  = suggest(space, past)
            fp  = {**tp, **fixed}
            tdf = run_backtest(opt_data, opt_tickers, fp, opt_filters, opt_cap, opt_mode)

            if tdf.empty:
                score = -999.0
                row   = {"trial":n, **{k:tp[k] for k in space},
                         "CAGR%":0,"Sharpe":0,"Calmar":0,"Profit Factor":0,
                         "Max DD%":0,"Win Rate%":0,"Trades":0}
            else:
                m, _, _ = compute_metrics(tdf, opt_cap, opt_start, opt_end)
                score   = float(m.get(skey, -999))
                row     = {"trial":n, **{k:tp[k] for k in space},
                           "CAGR%":         round(m["_cagr"]*100, 2),
                           "Sharpe":        round(m["_sharpe"], 3),
                           "Calmar":        round(m["_calmar"], 3),
                           "Profit Factor": round(m["_pf"], 3),
                           "Max DD%":       round(m["_maxdd"]*100, 2),
                           "Win Rate%":     float(m["Win Rate"].replace("%","")),
                           "Trades":        m["Total Trades"]}

            past.append({"params":tp,"score":score})
            results.append(row)
            if score > best_s: best_s = score; best_t = row.copy()

            if n % 3 == 0 or n == n_trials:
                tbl.dataframe(pd.DataFrame(results).sort_values(scol, ascending=False),
                              use_container_width=True, height=400)

        prog.empty()
        st.markdown(f"---\n### ✅ Done — Best {opt_metric}: **{best_s:.4f}**")

        if best_t:
            st.markdown(f"#### 🏆 Best Trial — {opt_strategy}")
            bc1, bc2, bc3, bc4 = st.columns(4)
            with bc1:
                st.markdown(mc("CAGR", f"{best_t['CAGR%']}%"), unsafe_allow_html=True)
                st.markdown(mc("Calmar", str(best_t["Calmar"]), big=True), unsafe_allow_html=True)
            with bc2:
                st.markdown(mc("Sharpe", str(best_t["Sharpe"])), unsafe_allow_html=True)
                st.markdown(mc("Profit Factor", str(best_t["Profit Factor"])), unsafe_allow_html=True)
            with bc3:
                st.markdown(mc("Max DD", f"{best_t['Max DD%']}%", False), unsafe_allow_html=True)
                st.markdown(mc("Win Rate", f"{best_t['Win Rate%']}%"), unsafe_allow_html=True)
            with bc4:
                st.markdown(mc("Trades", str(best_t["Trades"])), unsafe_allow_html=True)
            st.dataframe(pd.DataFrame([{k:best_t[k] for k in space}]), use_container_width=True)

        final = pd.DataFrame(results).sort_values(scol, ascending=False).reset_index(drop=True)
        final.index += 1
        st.markdown("#### All Trials")
        st.dataframe(final, use_container_width=True, height=500)
        st.download_button("⬇️ Download Results",
            final.to_csv(index=False),
            f"opt_{opt_strategy.replace(':','').replace(' ','_').lower()}_{opt_metric.lower()}.csv",
            "text/csv")
        st.caption("💡 Run Calmar, then Sharpe, then take the median across both result sets.")
