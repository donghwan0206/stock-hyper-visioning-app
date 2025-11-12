from __future__ import annotations
import requests
import json
from datetime import datetime, timezone, timedelta
import copy
import time
from typing import Dict, Any, List, Optional, MutableMapping, Mapping
from dataclasses import dataclass, field
import httpx
import logging


@dataclass
class AnticSignalCollectKIS():
    APP_KEY: str
    APP_SECRET: str
    BASE_URL: str = "https://openapi.koreainvestment.com:9443" #실전투자
    timeout : float = 10.0
    _token_expires_at: float = field(default=0.0, init=False, repr=False)
    _access_token: Optional[str] = field(default=None, init=False, repr=False)
    _base_headers = {
        "Content-Type": "application/json",
        "Accept": "text/plain",
        "charset": "UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }
    KST = timezone(timedelta(hours=9))
    # transaction id
    TR_ID_VOLUME_RANK = "FHPST01710000"

    # Auth
    def _issue_token(self) -> None:
        """요청 전에 토큰이 없거나 만료됐으면 새로 발급."""
        url = f"{self.BASE_URL}/oauth2/tokenP"
        payload: MutableMapping[str, Any] = {
            "grant_type": "client_credentials",
            "appkey": self.APP_KEY,
            "appsecret": self.APP_SECRET,
        }
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        # 만료 30초 전에 재발급되도록 버퍼
        self._token_expires_at = time.time() + int(data["expires_in"]) - 300
        logging.debug("KIS access token updated, expires_at=%s", self._token_expires_at)

    def _auth_headers(self) -> Mapping[str, str]:
        if not self._access_token or time.time() >= self._token_expires_at:
            self._issue_token()
        return {
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.APP_KEY,
            "appsecret": self.APP_SECRET,
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """공통 요청 헬퍼 – 필요시 토큰 재발급 후 JSON 반환."""
        url = f"{self.BASE_URL}{path}"
        merged_headers = {**self._auth_headers(),  **self._base_headers, **(headers or {})}
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.request(method.upper(), url, params=params, json=json, headers=merged_headers)
            resp.raise_for_status()
        return resp.json()
    

    def get_volume_rank_top30(
            self,
            fid_cond_mrkt_div_code: str = "J",          # 시장 구분 (J: KRX, NX: K-NEO)
            fid_cond_scr_div_code: str = "20171",       # 스크린 구분 코드 (명세서 고정값)
            fid_input_iscd: str = "0000",               # 종목코드 (0000: 전체)
            fid_div_cls_code: str = "0",                # 구분코드 (0: 전체)
            fid_blng_cls_code: str = "0",               # 평균거래량 구분 (0: 전체)
            fid_trgt_cls_code: str = "11111111",        # 대상 구분 코드
            fid_trgt_exls_cls_code: str = "0000000000", # 대상 제외 구분 코드
            fid_input_price_1: str = "",                # 입력가격1 (공란)
            fid_input_price_2: str = "",                # 입력가격2 (공란)
            fid_vol_cnt: str = "",                      # 거래량 수 (공란)
            fid_input_date_1: str = ""                  # 입력일자1 (공란)
    ):
        """
            거래량순위 API는 최대 30건만 반환.
        """
        api_path = "/uapi/domestic-stock/v1/quotations/volume-rank"
        tr_id = self.TR_ID_VOLUME_RANK
        
        params = {
            "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
            "FID_COND_SCR_DIV_CODE": fid_cond_scr_div_code,
            "FID_INPUT_ISCD": fid_input_iscd,
            "FID_DIV_CLS_CODE": fid_div_cls_code,
            "FID_BLNG_CLS_CODE": fid_blng_cls_code,
            "FID_TRGT_CLS_CODE": fid_trgt_cls_code,
            "FID_TRGT_EXLS_CLS_CODE": fid_trgt_exls_cls_code,
            "FID_INPUT_PRICE_1": fid_input_price_1,
            "FID_INPUT_PRICE_2": fid_input_price_2,
            "FID_VOL_CNT": fid_vol_cnt,
            "FID_INPUT_DATE_1": fid_input_date_1
        }

        r = self.request('get', api_path, params=params, headers={"tr_id": tr_id})
        collected_at = datetime.now(self.KST).replace(second=0, microsecond=0)
        data = []
        for item in r['output']:
            data.append(
                { 
                    'rt_cd': r['rt_cd'], 
                    'msg_cd': r['msg_cd'], 
                    'msg1': r['msg1'], 
                    'collected_at': collected_at,
                    **item
                }
            )

        return data