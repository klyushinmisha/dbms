package access

import (
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"log"
	"os"
	"testing"
)

var keys = []string{
	"GlNVLYANzD",
	"yRWZajETsL",
	"Ybssljnrti",
	"NluBqKhNJf",
	"MmICfXOsiV",
	"iOeufOQxDO",
	"PGoUqrSIrm",
	"CAAwrVJEAu",
	"WaOZBVnxyE",
	"vLkGQxlCba",
	"EFyZZDPNOY",
	"eyddPwIIZf",
	"JvlppnwXAD",
	"sOrkDCNhwI",
	"PlYmTcTozy",
	"fGGrvbJZhs",
	"VlYGVJMuAK",
	"aGPECkuUov",
	"tvvTlazwBe",
	"hKXBywObng",
	"LbzsDRRYPg",
	"qNQIgBwnJy",
	"fGoHdVuzNz",
	"syOhPHuHnG",
	"StTBpUVPVY",
	"AseqNfkrCn",
	"TWsAXxJvnf",
	"ETcrWwmRJP",
	"HGPQNtFFfy",
	"KQPRpsMaDl",
	"vSwbgstwgY",
	"hAuXQSbwYO",
	"xVOelfHvJM",
	"xhKKkKrDXr",
	"chCJBIWTWQ",
	"HZbiGtisHr",
	"JXKTbTYanY",
	"OUoJoTtIEh",
	"NXgLIZAcgk",
	"lTtCvkKTcY",
	"LrWDYJjDRY",
	"OWUshMQNgR",
	"rqYuqTmiid",
	"XXEdQAIoWE",
	"YOKwZVNykR",
	"EfnIENiRgw",
	"vvaioetVZF",
	"zitPzfDMPX",
	"wQqARpqJCQ",
	"SzhPCftpxc",
	"xFtfwNHDSx",
	"JCmSDkXVAs",
	"UGdILBtNoG",
	"qYbtybkHTE",
	"kiLHMUbFkw",
	"KIBbxwIIdh",
	"useHRGFWBa",
	"idWYKQOYJY",
	"artYIYeswy",
	"IXnqWRDWbg",
	"xjQpEcknhr",
	"yFROiMMSLV",
	"lcEWfuqhIn",
	"fciNMBnBFZ",
	"dobJkqXDhh",
	"ZjgfNJtojY",
	"SzEGWxkQvc",
	"okIQJCqBGh",
	"VkTdhlFeoO",
	"jQgSMPWZra",
	"AsPmqfqAaR",
	"ScFMOsFVKk",
	"RgeAoObqdr",
	"JpuHRAWGJG",
	"wvYUhFclyO",
	"vKxPfwxDEM",
	"YAnRDunSbU",
	"dzWXFcxNEL",
	"uNtPThPdWD",
	"yxqwneOGBy",
	"gEwrBkLWjE",
	"XtkQntVgWr",
	"vvLhRTzFgb",
	"hpUsscGzYh",
	"VOsozTVCUW",
	"qVaBKPvvHI",
	"zYmEKcoEEz",
	"lekNWfXsPw",
	"PtnPHAqJcu",
	"GusGZBMQpY",
	"MdDGgRubyg",
	"nCasCxomhU",
	"ZsuLYcplcO",
	"DNVURXVGGv",
	"wcdoDmaZZG",
	"eVJppHfpyY",
	"vtyixSiHfj",
	"VvwBYXztbb",
	"FXdOgbSkNL",
	"dTDTpDTrln",
	"hWosrzXfuK",
	"vZoxRDGUKV",
	"fyXzgfMJlB",
	"eFHPjxgDSK",
	"lIsoXecPRG",
	"wDRcKkTRqo",
	"CDtfBABVvC",
	"EeNhIrhsAS",
	"fNXnpSpmKc",
	"CvfOApWabM",
	"pGdQOEspOZ",
	"cRkopodNAF",
	"RVrcRyQcfh",
	"RJZQJkKgQa",
	"ygXtSbCTCu",
	"fckZUXNXaD",
	"sghFuDGOfB",
	"otghyvJVCL",
	"QljBKGjTYA",
	"IFDjYImCML",
	"cHIuDvBZMd",
	"LcSrNnOfLg",
	"nPlRTqaXXl",
	"vJmPbHcbxe",
	"aKCUmgyMzY",
	"wyLEKDPJMP",
	"kxKDAqcAow",
	"rkQVkWhFIE",
	"XFPrrOnNOt",
	"HkTPZFHRZd",
	"ArrqGAjwan",
	"zYilhLWbHT",
	"YbpPrTXPJc",
	"ffWyRHlEAY",
	"HGAttTDCmq",
	"MFvheoJZPA",
	"fgtwjxaFwQ",
	"iislaLaRuQ",
	"kpVeXcBoYM",
	"mtaqXUNffl",
	"RCYOEIaCaD",
	"mNykBJfqUc",
	"VNseSeYJqk",
	"qmZFXDKtRB",
	"yKTRvvyVfy",
	"xysapOIPEx",
	"fULZtQaofu",
	"wXcIzcCaka",
	"JUmibQgGtK",
	"oAPxikcqXr",
	"SnsEmyansd",
	"ndgTeMVSAj",
	"GYXSMPONzR",
	"AppXvaxTql",
	"WonwIlUeKt",
	"jeFWbNbDkb",
	"XDAeFMnJMR",
	"XESjEweWrS",
	"ITCHSBnmap",
	"VVgSqqkbFu",
	"kmsiqzZEZm",
	"ZpvgqzMQih",
	"IQEFTkGoWh",
	"TaXknoYJBs",
	"AXcnNjXsqJ",
	"ksHpWBFSXd",
	"YnoQwNfARv",
	"oYYkZExMwm",
	"oNxiGtpDAh",
	"pOLHLHwYGj",
	"UDuaakMOrK",
	"NisHcwQoTE",
	"iUhoZQJOVU",
	"fsDXNkzPcW",
	"CnqXOOBEcE",
	"VgWwkAQYQp",
	"rtCAagHvrj",
	"fbUSRFNCHK",
	"mnYIVmzDUW",
	"tkXRXLczEK",
	"yPHyrtirdZ",
	"ugvJFMrDbJ",
	"QCDdgQHrKH",
	"InvsTOKPXG",
	"jeMFrXygkA",
	"LMuYvxuXPu",
	"EiUsudFuYo",
	"EotgICNwLo",
	"PsZEqVnHQM",
	"oCkNiOIRux",
	"briUDCeiim",
	"ZbaZyxWkch",
	"bfeSoxhSeA",
	"FzadRKcWhD",
	"YeHbsnAhuj",
	"uRyFvQeyQt",
	"YFVvitNTux",
	"TROOLmTdPe",
	"girytwfmdR",
	"fjoWMKesQd",
	"JaHFTtpjyE",
	"GTJJsNyQxw",
	"kyAjPTwvcA",
	"dEaQhapjGf",
	"esmvEOJSgS",
	"KJDNZAfZJo",
	"eXjgFdGspz",
	"xpKJGxUBfU",
	"KjosSszPnR",
	"QYgpKqjfLw",
	"wepiVTEucG",
	"ebORRPBgEo",
	"nBuUjhaUVL",
	"HsWkpgQdCW",
	"acDnceyoIi",
	"rGGtGTSvGI",
	"iHEmuxUrZJ",
	"TQHtUyPTRD",
	"iOZAfckZnt",
	"FgavnMRlDW",
	"CdavJQWqka",
	"FwujZCTqVB",
	"WseNrErrbY",
	"Phdvtrdzbe",
	"xpjTIQnDGz",
	"hkdpYvPkjq",
	"SJWwQWjiKQ",
	"CJnTYOTaix",
	"YInxmWOFQw",
	"XgNnNIzPRV",
	"STzVABWvcx",
	"TSlflOdeMZ",
	"UeOCpaOEZu",
	"SZENFEpjKg",
	"tjDuIrFNVr",
	"IXtqgtYfrU",
	"VLJasPqBOx",
	"QABlqMmiGv",
	"ouHfcOUZEl",
	"usrIkgBsqp",
	"bbNSxckmsj",
	"BhBwnhwKsC",
	"euJPJIzlNY",
	"nYZcRicGkG",
	"xqZGkMJmsw",
	"atXVFqwQMB",
	"BIOFoEkPUH",
	"wkJWQMDmfS",
	"dJRiLxDUxS",
	"ONUbKrbnar",
	"lhtffgwcLC",
	"WMaVJfpkuU",
	"AKggRdvbdl",
	"dDmuJkjnJx",
	"rVkkuqzMOJ",
	"PwLKhOOaQx",
	"NyWlLKclEH",
	"gVtkHtjTYQ",
	"kJpNetZYGG",
	"RLDSoNrUCn",
	"orKLJZjHYw",
	"veWPkMJLWs",
	"yKCUAbjLeo",
	"WzDbnQGDqP",
	"fsMEKfxPtH",
	"VmZpPqJayi",
	"VGzxDdpcwY",
	"DGjxmnlasd",
	"CSINLdKbqj",
	"tCltwZPpDV",
	"IHlBDCJXoj",
	"cWQodjfLBm",
	"YQkMjkpdGr",
	"UlOOrybvqi",
	"vkxQApWpjV",
	"knzmknBXCJ",
	"NhggrWDOGv",
	"jGPpobiJHK",
	"HupePiFfYH",
	"vPpqwFVnVB",
	"dWsTDvgoRy",
	"ekIhvTvjbN",
	"bGklsSEaXj",
	"NbIdmaIgYt",
	"mFKpuZLimB",
	"KKnwtGALBZ",
	"jAsDIPtKpr",
	"OFmHhDubtP",
	"XkbVOUNPnB",
	"IoLPGsftrs",
	"VLThUlmfnJ",
	"TlSxHXAoFU",
	"iflufHZVxx",
	"yUorqeNkOr",
	"TuzIFvEPPj",
	"GQuFbZTUsV",
	"TgyPcFYzzI",
	"puYEVhPzdH",
	"TYuowgIdvE",
	"SfwihjrgtI",
	"CIxETiJlHB",
	"oSlRIDMAJh",
	"DiUCkWiDOq",
	"hKgCGJbFtB",
	"jcufDiZpUF",
	"VMgtwrffSs",
	"tWqluhtRiD",
	"GHGAfgfhpx",
	"dURohvfAam",
	"ImEYZWckRv",
	"xJxFuyZMaH",
	"LrLeZKVYqz",
	"ZQcJAwfDMf",
	"kKyXnxXXZQ",
	"uewGQOKsBf",
	"iloKktcWRE",
	"BvEkoVGraT",
	"nRMDbMIEAm",
	"MwvpIbdxwK",
	"qNSTGGpDUr",
	"vJZEtVtbrH",
	"gNEBJHveiH",
	"LyjtlFgjKX",
	"SPDTaUrVaa",
	"DbIWbERNsS",
	"VxaJmJkvIk",
	"tdMokCpaKS",
	"yiwuiGuvMY",
	"xTrYnRQhJk",
	"XxHLgwxJBF",
	"unXfmNaBKl",
	"TXKDtoYNIl",
	"PbhyANRywx",
	"HFNfyBLJWe",
	"ZqjxmXzrDj",
	"aispGCurRX",
	"TkMSySLGbQ",
	"PCIvdgsJdc",
	"YkZAizqfFR",
	"xGMdPBMAoN",
	"gIYvjddIFh",
	"vrozvqgIVg",
	"nodQSMvPKu",
	"zUOWcBsjam",
	"rDtHJYzZRL",
	"LhoorpUBkU",
	"RCOiwMGFAG",
	"BFGSVPiXJL",
	"EEwToukiuP",
	"zCzWxEbKcj",
	"RlTTbAiUnx",
	"qEVhGojFrh",
	"ILibrbBMaW",
	"VFGqlsOlut",
	"nUuJhtToGs",
	"jidUYoNnro",
	"OBswFejwYe",
	"rsKmqmtoUt",
	"DluWgYtHql",
	"FmTzKItkPG",
	"ZhFXdwLUwd",
	"WifOtyEWge",
	"udEyeiROTW",
	"MiOUXuvtZG",
	"TjHYydMpKG",
	"angfdlTsju",
	"axvwTmTeaF",
	"tZMuhzedvu",
	"OrleSkQetn",
	"vQALOlRhDi",
	"ygfGSnLkAz",
	"jycGCTRpLT",
	"hfwKMEpEMS",
	"yDoRPAFGSM",
	"ImXxLdtsoy",
	"ngsNfDMZRh",
	"dnlqmXLrgx",
	"mgcWvBPDQK",
	"RicqkANASR",
	"WAtSguZuvg",
	"WckdBWTtBY",
	"EzWGOQFKod",
	"eKVqXZYAVO",
	"csRHlHknlE",
	"EhfNWAvXfa",
	"DsIfULUVVk",
	"zEBpPnGexT",
	"JPVvpzBrSV",
	"WxiwTkJXFv",
	"AXuzjEnrnr",
	"SQQzxpxdIS",
	"YIGXxzeBDG",
	"DnIQPkihYW",
	"gMcDdqnmCS",
	"kVMODDICYK",
	"nTzFLgSxqO",
	"aZOdUYnotB",
	"WIKGaxOYhN",
	"XFCmSQSPPu",
	"MPpXdTUbIt",
	"NHrQpykSnN",
	"nYYFjlzhpW",
	"FzxBueZufU",
	"ayGmeDUlNR",
	"WFCixMdZPf",
	"udTmjhrSsW",
	"ztcrMRqlHP",
	"SscnnKjyFv",
	"mEoVJVwMlB",
	"rfTYuvzhcF",
	"hWPBGcDeaP",
	"KRLIlcBVBS",
	"rcXPxgPoHe",
	"uMTEjhmrhn",
	"ucYjEfBVtT",
	"dEYEUHIkKL",
	"txEEMXuafQ",
	"qsIynDrNqy",
	"pEPtjSJzUm",
	"TNCQSHtkLZ",
	"dnWdWiZWmn",
	"RVuFDjpeBR",
	"UzCRqmUUUq",
	"kACvrVtIvm",
	"wwgAubOhcm",
	"QNFAiGOLbg",
	"AapoYGkyDX",
	"oykdcJPKJc",
	"OqBcbtaVKJ",
	"IeyTrjTFOm",
	"LwXbweqoTq",
	"RaFuOVUNTI",
	"cKOqqVxyYF",
	"fwJrBoQIiy",
	"rXyRANtDDC",
	"BKMoMLDlMT",
	"WoJroCDDLF",
	"XBFqoxfZGM",
	"qWWTErxwfA",
	"RBbyFVouLQ",
	"vRxAMUlHOy",
	"sknewNSGbu",
	"jJiIScqekO",
	"bJvseNvfBk",
	"gHGmsNqRFP",
	"qGHeapWzRi",
	"DxUfJbxLWY",
	"yaYpiysxGy",
	"uXJFXdOpct",
	"hQlxoZxtwl",
	"GRQcPBXTYS",
	"VdFSMKzkwb",
	"mIXsWUnJqQ",
	"iKfktxJEqy",
	"gyCHkORtqc",
	"ukxaiOTcYW",
	"HLpEAhAIXW",
	"YltMGLPOkP",
	"POUSCVzdcj",
	"OEupTAVhYF",
	"seDhigsSCQ",
	"PMcXoQmdaB",
	"YQkgXRxxKw",
	"dZsJGgyKfH",
	"eWVDUfNStX",
	"KCmheQpqWa",
	"oJACfsokah",
	"FmAcmaZtia",
	"KOZSTipxit",
	"HodjLwyhYz",
	"lBixzzUrDB",
	"ghJqFoNSSI",
	"GXhtiLHSgo",
	"oHdViIKmih",
	"JPuqVoqWFB",
	"DTnjsrapiw",
	"tlwYiqVCBs",
	"mjCIJbFhmd",
	"PdrFrdDByb",
	"suEnFBwHvd",
	"FJLURuaJOo",
	"VwmmfFCBII",
	"NaJyDbjLSY",
	"UNIEnlfLCM",
	"pBwJPMdOWG",
	"PRpkoaqDIK",
	"lXaYGwMHIa",
	"kKkKsUwiGB",
	"fsszqzQaxG",
	"aIuycFsxIX",
	"eKlEoNjBqz",
	"uEnUeobUWW",
	"ukVKRJpnIq",
	"pJzvFAYJKZ",
	"LEVUklldlB",
	"UtWFZpjrjC",
	"simqPEcbsX",
	"GBDgrMwEKd",
	"lsvEkdoGDX",
	"CKMEvjJtWW",
	"DQHCWoYtWA",
	"yMJAGJYPFh",
	"ZwRrgHhknP",
	"pbxVjQQsMm",
	"jyCPeAynYP",
	"nsDpMuskQs",
	"VhsdERtNEN",
	"iWroVALtkm",
	"yUwyUUqmXU",
	"nfZyPenvcf",
	"JiggsOmGMd",
	"QmXbOdZOvH",
	"bCHMmDRwGr",
	"lsyCBCdZTx",
	"sZFFhXLCjY",
	"XiqNuMzCNU",
	"gnqmtEMOAJ",
	"eDCULdrKIn",
	"ySHmaQQVPn",
	"jbIqfPfiaL",
	"tqeeYEjZKb",
	"TaUqCiDhLU",
	"FsFhsOGSKT",
	"UeLICJPJiN",
	"OcAvbiQRXT",
	"RQoFjVLkff",
	"YucAsiAXzN",
	"tfEntPJoFX",
	"ogSpVqiDxS",
	"oUPORfdvQy",
	"oSkfORVcww",
	"jEJVEahHDZ",
	"LOPzAgrVxF",
	"gpbmBSIPyV",
	"awkMBuksMN",
	"sExTyqDdmE",
	"BVUHWMADuM",
	"gFYZuMwahY",
	"UEQauqRBHX",
	"PosoOreFbX",
	"aljvwNKFir",
	"kIVMeCUiWz",
	"WmmAHerWnM",
	"mojFEcegbb",
	"qguCyTUzmf",
	"gLHnuZVnZk",
	"rxqpmQbsUg",
	"MwgzFRTopb",
	"LbYRFAOGgO",
	"QLyBzlffLk",
	"mnAfILTdrv",
	"cvVifvtiBl",
	"ZXOJsLzvVF",
	"kFjPbFhcmj",
	"kiDpjmXVeK",
	"UOAaqompzW",
	"WtoIcNciIC",
	"NYVuinOmDB",
	"HOQJutmVfq",
	"DQuKhiJQIc",
	"rqPACcTDRs",
	"RjqzmddHcV",
	"lsMVajPuvQ",
	"HVkLxoHnql",
	"OlxHQwygTd",
	"burqBrnbRb",
	"gjXfmScVTI",
	"xaTTzcSoYf",
	"DUQOrPkTrN",
	"ftDoNGyHxx",
	"FLPsHDnhyW",
	"OFnMuGnMpp",
	"ijBGVsaWCU",
	"AnarQchamf",
	"GRtfbBGIud",
	"cUhazNmVqE",
	"OrMrdfmmLS",
	"yLeLMuBJaD",
	"CwYoMVouOl",
	"eXgisPISCU",
	"oWXUukXikF",
	"qTzeetwLaN",
	"ltRzQGHlTi",
	"BhzPDnKrGe",
	"LxoCnTxWHM",
	"eRViCekXTz",
	"uQectnBVet",
	"ZGMURiLbwu",
	"sXlKUkpKVm",
	"hjfJiBnEHd",
	"oDgLWAgzPD",
	"nmoDkeafeK",
	"KFiDwDzAcy",
	"BYqTjQMTPy",
	"BsTonDzBlr",
	"IMRzPvZRlN",
	"ttXfxFKMly",
	"dEmaLKbPnP",
	"DDqetFRdRs",
	"soThCXwHij",
	"mfIqTqPHKa",
	"oEQtwDOcyO",
	"NJyEbcjXoY",
	"eKkaXIFeYn",
	"PruEdIcmva",
	"fszPAUVMVW",
	"WevwsktmTp",
	"OFchVntvZE",
	"AzXbhqlmGK",
	"hylBVEfeyW",
	"JPQDWXFCcB",
	"FTsvNakcjG",
	"EfgLgYzWbO",
	"ZplMYMfeFv",
	"xCBjkSVYFj",
	"BAqCsYRcIo",
	"AAfiVKLzyq",
	"cKbWZqvCNK",
	"wrIolwYfBV",
	"vHHfHLJBmX",
	"AzsQrgqpHU",
	"DyvOuqubyc",
	"NinaahkdBz",
	"xrMfVDWMsz",
	"AyJljyhgIU",
	"zPSHceXpOp",
	"myYAdmQxWa",
	"igprhIpKsz",
	"pUulTQxyOG",
	"YAHQOADmZE",
	"HgheeuRgnL",
	"chKkmSUKye",
	"EXEAMlYXsn",
	"jpqKpQGImI",
	"FzwHcyYYCb",
	"CRFLbRCxTy",
	"fuakjqrRVP",
	"PlkEHsitOB",
	"thIbeHzSbX",
	"GRqobftXAG",
	"DJKfDObvBz",
	"WkzYzCwKaF",
	"ZtEtkPudCx",
	"iLHrEDtcRJ",
	"YjfBliURPy",
	"ADxOcmBQXz",
	"CHbrKxYpOe",
	"nEHGOUltEM",
	"PhuGamZlRi",
	"nXHtmtElIE",
	"fWIILblZQH",
	"GHXXvkyULK",
	"AKSLmnDEMt",
	"zVWpUntghp",
	"MTUBpzEbgu",
	"QcsSjAozvz",
	"HayWPwVILZ",
	"IboKlKCTSo",
	"ntlNeNAtTr",
	"fMGJtbbbcM",
	"FwDDWcImkO",
	"OFsUJzYPQe",
	"Amwkhhjglw",
	"MgRRvTxYxD",
	"RqNHYmrKRB",
	"wjzXwQNpww",
	"RIEaDKDMoE",
	"ThFOeNyMsG",
	"JTjmVrKxGL",
	"aMVCHVpAjw",
	"HHyNZKCOrl",
	"EMNoFWFBYf",
	"YAtFybrVxZ",
	"VirDONuYUT",
	"noWhTTrgFQ",
	"yizXUTTMoh",
	"uJlmTUeltR",
	"erNqxEUCDi",
	"HLkDVTkDHI",
	"CPCHlveysm",
	"XSHUhGWoLP",
	"bDjTKDTxfI",
	"VUHOhBTOqz",
	"wufJijaxTJ",
	"EqymHDcuSR",
	"OzZPooIqnM",
	"ZSXCwDkMdI",
	"sRgWPOkYza",
	"XqcBUAIREv",
	"QBzHkWwnim",
	"NaQpfNNysZ",
	"CgCfgubrzF",
	"AifhAJrfZT",
	"ztDddddYkO",
	"wTxlBrxLXT",
	"WpVfWPBhEx",
	"zJvdGkUpKW",
	"IDwMEqmHBN",
	"WJHSdMeUeL",
	"LsnRCRLqdT",
	"MpcrzkqlQy",
	"kKqUcIdtWr",
	"cfVgBuDEAJ",
	"ueVJhxgSYA",
	"nCJIWaOISR",
	"ArbjlCxXUz",
	"cuRCJgCqoJ",
	"GVZEqmteXA",
	"wRBubXSVWn",
	"RGZqnLzluR",
	"aaOUzVVACS",
	"quSiCgMsTB",
	"bnzlHvfZzT",
	"qgyfHYGWkA",
	"wQGprYEHEr",
	"thmuhYpyTD",
	"ShOdcuSNsr",
	"qoIaojXBQR",
	"luKAmytBrF",
	"vktIdaaFWu",
	"NQQSeNFCsY",
	"xabmyKcNsc",
	"YjEDepEYfz",
	"BTKuzpkwbV",
	"hTSWDXxdzb",
	"dKcbkUdnYd",
	"PBGnjXjYVo",
	"MepwCKSiDm",
	"pMkoHChTTb",
	"TWViCpcasI",
	"TKRgxVrgwg",
	"MyhmrfAkie",
	"qPqPaWmzxB",
	"ngbLdxtshz",
	"uCnlPHFnPe",
	"SWbjYVaoJT",
	"wHVtemPiTq",
	"zlGHnYconu",
	"qCjCUxQlDR",
	"QHLLBFypFg",
	"PRUBsFEArH",
	"bPgJXWxLcU",
	"plrDWfNMSC",
	"iGeEWmTwle",
	"nkrdHGzuLQ",
	"EGtbhfxPLe",
	"XoUSwlIStC",
	"cRUPcRtDuF",
	"VeZaDBqJav",
	"YpMRUSZauc",
	"xOidXRlMqE",
	"jyrAyyXSmo",
	"HXjWVbejEt",
	"jaJBJEqSjM",
	"uXcmzGeUNW",
	"EynZTHmLxH",
	"PzSBFbcyZV",
	"WwGqeVzQtX",
	"TezgqlCoAh",
	"coPXJNDquU",
	"kHeXYUANpZ",
	"IoJsiDaukW",
	"mHuzKpNPXc",
	"vfngfccjxt",
	"zxzXmTCsFC",
	"WHFwzVQgMl",
	"AUYdsvFtlS",
	"zIrnfLAhjk",
	"NpWKPvFXaD",
	"gMtNkbZPSo",
	"hJIruTKgfr",
	"VXasozMYAN",
	"yzZUeKLimi",
	"vRsbPDAaJW",
	"cMiEjNRnAr",
	"sohUsTKLcm",
	"ozOvQXmShu",
	"JDGhGJDFDx",
	"sFdBSXfLUr",
	"lfWZquoAoY",
	"RPQiKfzRoZ",
	"IzZXrlBkkh",
	"GfesrZnUyG",
	"zskfmRBkDX",
	"lkkTJopfUF",
	"sZFXcbNlme",
	"mNePCvokBj",
	"hDVQYpHJwD",
	"WQaLRRWpZh",
	"WfLXPtAYAJ",
	"ChALozmgPY",
	"NrmOMkDZhu",
	"KrRDMtiuGy",
	"CfaCqwtOQB",
	"AqeiubfVHm",
	"bxwoPRtcXe",
	"cOvKBHrzGc",
	"eycDYYzSAy",
	"ihPZJczroI",
	"RfKJvCNNss",
	"VrmitqXpvm",
	"OXmJTAcVAD",
	"pkHoNZWnjb",
	"pxqsNfSJKu",
	"WBRVGCKAif",
	"zLqDgzKarI",
	"geKarvOhhB",
	"nHoQcpKnzu",
	"ZKkGayVObM",
	"qEAeDWdYlF",
	"GyYCwcmuja",
	"sWpwsyDgKr",
	"UnhMgQoyGn",
	"YOJROTAIzC",
	"xfuOEySIrC",
	"RWkMqFGTVS",
	"XaWUhJkvWK",
	"gCUPUoYDIT",
	"yEcPLNrryu",
	"mhaIgcuAKn",
	"AuxvTiApkG",
	"cAxmxUseRA",
	"fOjtRuKMqs",
	"wxNpdNlVXb",
	"bNJHlCoHUC",
	"SVtEtDcBxQ",
	"hEsLWvLuwW",
	"KytisRCAdO",
	"vCkvVJfYTS",
	"awtoJiWOzf",
	"XtctyrorPM",
	"TOUatHlpVh",
	"SaVCuFPhnR",
	"ZalquBGzTA",
	"ZKdNTRymFI",
	"ggSycJAMcJ",
	"hwSwNpQmtF",
	"ccaQBhZqnR",
	"WAZzFiizoY",
	"LwoXWsBUQR",
	"VDFGbCfwMd",
	"sxKIfKYQNo",
	"lkNPHIRxii",
	"UefxKvvlDi",
	"FvlTUwbfmD",
	"efMQNvUdWP",
	"hPOnthKBbX",
	"EmxzxRaWOy",
	"bDgXwoKCEO",
	"PWKRMCwuMi",
	"DmDMfBqCrC",
	"fzkXtRpGxk",
	"TqmPBfAvMO",
	"TNHCAuZpRu",
	"rhyviwdLKV",
	"DweRyzPsOx",
	"fgQwdletgU",
	"LxlViwAydY",
	"vILOIqYZqG",
	"lgeThEMPVP",
	"iJWcpmkDqC",
	"flZrTLAaly",
	"OvSggKoJFq",
	"PcvyFcgxhi",
	"oTnVAQHlZo",
	"fzSruMqsGn",
	"dCgTZESGjE",
	"rScJcFxVss",
	"StoXksWqRC",
	"IBwjoBPyHk",
	"HxYybamADf",
	"JyqbJbNbxT",
	"lMTgwvLmBX",
	"hAgvQQTzCn",
	"eUsXQgAOPH",
	"TNAoaimJzj",
	"BnitzpgssI",
	"XeyixHkQdV",
	"obRFkNQHeG",
	"dLfVKgtiNF",
	"zmMISmnGNT",
	"zIVFmoarez",
	"XXmsxxVlrN",
	"bxzvpDANdC",
	"InLJqutANB",
	"leFXqGUbFt",
	"EfoUVbefuJ",
	"LKuUUBcCYb",
	"yCVhudTlCN",
	"eqLjeJjJjC",
	"fXZDyRHYWf",
	"VdrcnZRRAR",
	"odyWYqyzcQ",
	"lfLGonqKpL",
	"fRcAgqlhJA",
	"NZGQhWMKqt",
	"ZPTbBPZenK",
	"IxdzPdoFpt",
	"vUiYSgWEog",
	"wIecyEsivS",
	"YFFxlmMPdG",
	"zfoMZHQZFG",
	"jekjAzOFbU",
	"nuMTwGZtJk",
	"mMMgNTeLwq",
	"PlzsXPzgjw",
	"acSoGeFoaC",
	"SHDpgzOWJv",
	"xYfRPPkkga",
	"ZDDnsosEiV",
	"JkWQQxNAIW",
	"lxSlRoqVIV",
	"ThSpjTIXNl",
	"GenWzyivGO",
	"VlYsqtOXAG",
	"TOUWswVwoM",
	"TlIROldwmS",
	"BnuACqYAyT",
	"VYFCjebbwE",
	"BAmwbvxZie",
	"MIazFoVwgQ",
	"vdmNiVRRDK",
	"PjkJWupleJ",
	"CQIyOnmzzM",
	"ZZXbvdjKhJ",
	"noOTRwnmUJ",
	"CdquyvlZil",
	"BKiYYehpuP",
	"dYDGTWTMxo",
	"VjlzaeMtJs",
	"uXEyUEIPCV",
	"qlkRvueKlG",
	"yzXdDmioxx",
	"gVbIVGCIax",
	"zapvHGZWGg",
	"lzVqXJzDGQ",
	"EAFkzNaYhD",
	"gByvYyrDyE",
	"ksNdUcIyLk",
	"ZWgpsRxFVr",
	"fGppdagIrQ",
	"ovmeQTJcpR",
	"izOrVQDOUh",
	"qBRNcHEXmc",
	"FLAjmUBkDo",
	"AAwFiybgmO",
	"jQCjCcDInr",
	"CtHvJlDpDM",
	"MquddjXMrz",
	"hSRqOFJRIm",
	"paPZWhvsIn",
	"YRbFLLrsGo",
	"ekGyfdJyzP",
	"XYPGqeevUy",
	"AirNjEGfEv",
	"uzCTzcMbgc",
	"wnPyGhjWdI",
	"mhTOkEkjXV",
	"iDNQRPEuho",
	"iIpeyADgIC",
	"IWOUwVpeMA",
	"ZXvLTDogEF",
	"MgNSCuDdAB",
	"dQrPcaBDfw",
	"LpPEZbJpNH",
	"oeRcNxwAOn",
	"qXwzwaoKMx",
	"bPfWNLPFpH",
	"bDItrfnjOk",
	"XpYDiQlpAX",
	"znWGydNnvM",
	"EyyMAamNJz",
	"ojZvlBqhwI",
	"rmzmcUXWlP",
	"PRHsyagzua",
	"mFOgRhhsnW",
	"fUIEajurMG",
	"fgumnGMRrO",
	"EingUDxUWk",
	"LfPCjzdpwz",
	"fDNocdnmHR",
	"yLhIkTpejM",
	"XGxrLdUbzO",
	"ACrVGyapOK",
	"qBDLQdNWeP",
	"eLnirAPHgw",
	"XuTSXcXpMy",
	"iwnLUDQUXI",
	"sOToiBAYew",
	"guLiiNqdjH",
	"ELjZklpoYN",
	"rjhWITwkAd",
	"AhYJxsnWyy",
	"RmmzzUEzJH",
	"vrECXgRCWM",
	"nEGyJYIfaT",
	"gcGMrMRHwZ",
	"IaAxkzfrLD",
	"WHnZbKHUnJ",
	"EXpvYVAVGp",
	"duJEXMuUvy",
	"hmoMrvJwVA",
	"qencJBCySd",
	"sMskJeIeBW",
	"QbQMaMYtbI",
	"CvWAdGRiOB",
	"JWSEAKHQZK",
	"UVgSqVKehc",
	"GDJnMyDoKy",
	"AfdyAyyigS",
	"VdnzPVZppa",
	"kdLRmzhjVt",
	"BrqFMYAFZV",
	"saSufdPdAS",
	"gSBbfPNYqM",
	"kOiVBqbodk",
	"nCTXtKbZKQ",
	"tuPLzluRJW",
	"mNarHCmSQx",
	"iDxjsaBvmi",
	"HBVWdueKft",
	"ZwdZjnFzQo",
	"vTXxwSLlaF",
	"QSWyjIdcEl",
}

func TestBPlusTree_Insert(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(file *os.File) error {
		disk := storage.NewIndexDiskIO(storage.MakeDiskIO(file, nil, nil, 4096*2))
		tree := MakeBPlusTree(100, disk)
		tree.Init()
		for _, k := range keys {
			log.Print(k)
			tree.Insert(k, 0xABCD)
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBPlusTree_Find(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(file *os.File) error {
		disk := storage.NewIndexDiskIO(storage.MakeDiskIO(file, nil, nil, 4096*2))
		tree := MakeBPlusTree(100, disk)
		tree.Init()
		for i, k := range keys {
			tree.Insert(k, int64(0xABCD+i))
			_, err := tree.Find(k)
			if err != nil {
				log.Panicf("Key %s not found\n", k)
			}
		}
		for _, k := range keys {
			_, err := tree.Find(k)
			if err != nil {
				log.Panicf("Key %s not found\n", k)
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBPlusTree_Find_Non_Existing(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(file *os.File) error {
		disk := storage.NewIndexDiskIO(storage.MakeDiskIO(file, nil, nil, 4096*2))
		tree := MakeBPlusTree(100, disk)
		tree.Init()
		for i, k := range keys {
			tree.Insert(k, int64(0xABCD+i))
		}
		invalidKeys := []string{"Z", "H", "J", "W", "K"}
		for _, k := range invalidKeys {
			_, err := tree.Find(k)
			if err != ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBPlusTree_Delete(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(file *os.File) error {
		disk := storage.NewIndexDiskIO(storage.MakeDiskIO(file, nil, nil, 4096*2))
		tree := MakeBPlusTree(100, disk)
		tree.Init()
		keysToDelete := keys
		for i, k := range keysToDelete {
			tree.Insert(k, int64(0xABCD+i))
		}
		for _, k := range keysToDelete {
			_, err := tree.Find(k)
			if err != nil {
				log.Panic(err)
			}
		}
		for i, k := range keysToDelete {
			err := tree.Delete(k)
			if err != nil {
				log.Panic(err)
			}
			_, err = tree.Find(k)
			if err != ErrKeyNotFound {
				log.Panicf("Found deleted key %s", k)
			}
			if i == len(keysToDelete)-1 {
				break
			}
			for _, k2 := range keysToDelete[i+1:] {
				_, err = tree.Find(k2)
				if err != nil {
					log.Panicf("Not found untouched key %s during %s delete", k2, k)
				}
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
