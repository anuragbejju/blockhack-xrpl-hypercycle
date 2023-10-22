from fastapi import FastAPI, HTTPException
from starlette.responses import Response
import io
from pathlib import Path
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, File, UploadFile, Form
from pdf2image import convert_from_bytes
import spacy
import delegator
from lxml import etree
from wand.image import Image as WImage
from wand.drawing import Drawing
from wand.color import Color
import utilities.read_xml_json as pdx
from utilities.read_xml_json import auto_separate_tables
import delegator
import warnings
warnings.filterwarnings('ignore')

from xrpl.wallet import Wallet
from xrpl.constants import CryptoAlgorithm
from xrpl.account import get_balance
from xrpl.clients import JsonRpcClient
from xrpl.models import Payment, SetRegularKey
from xrpl.transaction import submit_and_wait
from xrpl.wallet import generate_faucet_wallet
from xrpl.clients import JsonRpcClient
from xrpl.models import Payment, Tx
from xrpl.account import get_balance
from xrpl.clients import JsonRpcClient
from xrpl.models import Payment, SetRegularKey
from xrpl.transaction import submit_and_wait
from xrpl.wallet import generate_faucet_wallet
import nest_asyncio
import asyncio
import PyPDF2

import concurrent.futures




app = FastAPI()

upload_folder = "pdf_uploads"
Path(upload_folder).mkdir(parents=True, exist_ok=True)

nlp = spacy.load("en_core_web_sm") 

def join_tx(x):
    return ' '.join(x)

def submit_payment_sync(sender_xrp_address, amount_XRP):
    from_seed = sender_xrp_address
    from_address = "rEGugmgEogLNi7ZcCkFAtMNrbqvZ7VAQAE"
    to_seed = 'sEdSWLRVb5zbjEv5GpzkLMcY6XkUbU9'
    to_address = 'rNGukvjVrQhMpFXWMD4Sxvc2mxHMmJtcHJ'
    from_wallet = Wallet.from_seed(seed=from_seed, algorithm=CryptoAlgorithm.ED25519)
    to_wallet = Wallet.from_seed(seed=to_seed, algorithm=CryptoAlgorithm.ED25519)

    client = JsonRpcClient("https://s.altnet.rippletest.net:51234")
    amount_XRP = str(int(amount_XRP)*1000000)  # Amount to send in XRP (as a string)
    payment_transaction = Payment(
                account=from_wallet.address,  # The sender's XRPL address
                destination=to_wallet.address,  # The receiver's XRPL address
                amount=amount_XRP
            )
    payment_response = submit_and_wait(payment_transaction, client, from_wallet)
    tx_response = client.request(Tx(transaction=payment_response.result["hash"]))
    print(tx_response)
    print(get_balance(from_wallet.address, client))
    print(get_balance(to_wallet.address, client))
        
        
    return tx_response

  
def get_wallet_balance(address):
    client = JsonRpcClient("https://s.altnet.rippletest.net:51234")
    return get_balance(address, client)




def find_label(text):
    doc = nlp(text)
    for ent in doc.ents:
        return ent.label_
    
    
# Load the English language model
 # Replace with the specific model you downloaded



class Extractor(object):
    def __init__(
                self,
                pdf_loc="",
                page=1,
                dist_threshold=25,
                ver_prominence=None,
                hor_prominence=None,
                **kwargs,
                ):
        self.pdf_loc = pdf_loc
        self.page = page
        self.dist_threshold = dist_threshold
        self.ver_prominence = ver_prominence
        self.hor_prominence = hor_prominence
    def get_pageview(self, save_img=False, file_name=""):
        """
        View page to be extracted in image format
        ----------
        Input : Object
        Output : Image
        """
        img = WImage(filename=self.pdf_loc+"["+str(self.page - 1)+"]", resolution=300)
        img.background_color = Color("white")
        img.alpha_channel = "remove"
        img.format = "jpeg"
        if save_img:
            self.save_image(img, file_name)
        return img
    def _create_coordinate_from_html_table(self,
                                           pdf_text_path="pdftohtml"
                                           ):
        """Function to recursively parse the layout tree."""

        cmd = """{0} -xml -fontfullname -nodrm  -hidden  -i -f {1} -l {1} {2} output.xml""".format(
            pdf_text_path, self.page, self.pdf_loc
        )
        a = delegator.run(cmd)
        b = delegator.run("cat output.xml")
        xml_op = b.out
        b = delegator.run("rm output.xml")
        return xml_op

    def _create_coordinate_table(self,
                                 pdf_text_path="pdftotext"):
        """Function to recursively parse the layout tree."""

        cmd = """{0} -bbox-layout -enc UTF-8 -f {1} -l {1} {2} -""".format(
            pdf_text_path, self.page, self.pdf_loc
        )
        a = delegator.run(cmd)
        return a.out
    
    
@app.get("/")
def root():
    return {"message": "Fast API in Python"}



@app.post("/upload")
async def upload_pdf_and_return_redacted_image(file: UploadFile = File(...), 
                                      page_number: int = Form(1),
                                      sender_xrp_address: str = Form('sEdVTDBqdFCYMVRexzeDKxXSJuXBkbq'),
                                      xrp_tip_amount: int = Form(0),
                                      
                                      line_token: bool = Form(False),
                                      Person: bool = Form(True),
                                     Numeric: bool = Form(True),
                                      Organization: bool = Form(True),
                                      Country: bool = Form(True),
                                      Date: bool = Form(True),
                                      Custom_Text: str = Form(''),
                                     
                                     ):
    try:
        file_path = f"{upload_folder}/{file.filename}"
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())
        print (file_path)
        pdfReader = PyPDF2.PdfFileReader(file_path)
        if xrp_tip_amount > 0:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                response = await asyncio.get_event_loop().run_in_executor(
               executor, submit_payment_sync, sender_xrp_address, xrp_tip_amount)
            
            
        table = Extractor(pdf_loc=file_path, page=page_number)
        
        

        # By default does not save image
        xml_doc_html = table._create_coordinate_table()
        xml_doc_html_df = pdx.read_xml(xml_doc_html, encoding="latin-1", transpose=True)
        xml_doc_html_data = xml_doc_html_df.pipe(auto_separate_tables, [])
        #datapoints = xml_doc_html_data["text"]
        datapoints = xml_doc_html_data["doc"]
        
        if line_token:
            datapoints = datapoints[["page|flow|block|line|@xMin", "page|flow|block|line|@yMin",
                        "page|flow|block|line|@xMax","page|flow|block|line|@yMax", 'page|@height', 
                        'page|@width','page|flow|block|line|word|#text']].groupby(["page|flow|block|line|@xMin", "page|flow|block|line|@yMin",
                        "page|flow|block|line|@xMax","page|flow|block|line|@yMax", 'page|@height', 
                        'page|@width']).agg({'page|flow|block|line|word|#text': join_tx}).reset_index()
            datapoints['label'] = datapoints['page|flow|block|line|word|#text'].apply(find_label)
            img = table.get_pageview()
            datapoints['x1'] = (datapoints["page|flow|block|line|@xMin"].astype(float)  * img.width) // datapoints['page|@width'].astype(float)
            datapoints['y1'] = (datapoints["page|flow|block|line|@yMin"].astype(float)  * img.height) // datapoints['page|@height'].astype(float)
            datapoints['x2'] = (datapoints["page|flow|block|line|@xMax"].astype(float)  * img.width) // datapoints['page|@width'].astype(float)
            datapoints['y2'] = (datapoints["page|flow|block|line|@yMax"].astype(float)  * img.height) // datapoints['page|@height'].astype(float)
        else:
            datapoints = datapoints[["page|flow|block|line|word|@xMin", "page|flow|block|line|word|@yMin",
                        "page|flow|block|line|word|@xMax","page|flow|block|line|word|@yMax", 'page|@height', 
                        'page|@width','page|flow|block|line|word|#text']]
            datapoints['label'] = datapoints['page|flow|block|line|word|#text'].apply(find_label)
    #         # Read the uploaded PDF file

            img = table.get_pageview()
            datapoints['x1'] = (datapoints["page|flow|block|line|word|@xMin"].astype(float)  * img.width) // datapoints['page|@width'].astype(float)
            datapoints['y1'] = (datapoints["page|flow|block|line|word|@yMin"].astype(float)  * img.height) // datapoints['page|@height'].astype(float)
            datapoints['x2'] = (datapoints["page|flow|block|line|word|@xMax"].astype(float)  * img.width) // datapoints['page|@width'].astype(float)
            datapoints['y2'] = (datapoints["page|flow|block|line|word|@yMax"].astype(float)  * img.height) // datapoints['page|@height'].astype(float)


        with Drawing() as draw:
            draw.fill_opacity = 0.1
            draw.stroke_width = 3
            draw.stroke_color = Color("red")
            draw.fill_color = Color("red")
            draw.font_color = Color("red")
            draw.stroke_dash_array = [0]
           # print (datapoints[datapoints.label.isin(['CARDINAL'])])
            all_pii = []
            if Person:
                for idx, val in datapoints[datapoints.label.isin(['PERSON'])].iterrows():
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            if Numeric:
                draw.stroke_color = Color("green")
                draw.fill_color = Color("green")
                draw.font_color = Color("green")
                for idx, val in datapoints[datapoints.label.isin(['CARDINAL'])].iterrows():
                    
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            if Organization:
                draw.stroke_color = Color("yellow")
                draw.fill_color = Color("yellow")
                draw.font_color = Color("yellow")                
                for idx, val in datapoints[datapoints.label.isin(['ORD'])].iterrows():
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            if Country:
                draw.stroke_color = Color("orange")
                draw.fill_color = Color("orange")
                draw.font_color = Color("orange")   
                for idx, val in datapoints[datapoints.label.isin(['GPE'])].iterrows():
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            if Date:
                draw.stroke_color = Color("blue")
                draw.fill_color = Color("blue")
                draw.font_color = Color("blue")  
                for idx, val in datapoints[datapoints.label.isin(['DATE'])].iterrows():
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            if len(Custom_Text) > 0:
                draw.stroke_color = Color("purple")
                draw.fill_color = Color("purple")
                draw.font_color = Color("purple") 
                c = list(Custom_Text.split(','))
                print (c)
                for idx, val in datapoints[datapoints['page|flow|block|line|word|#text'].isin(c)].iterrows():
                    draw.rectangle(  val.x1, val.y1,  val.x2, val.y2)
                    draw.push()
            draw(img)
       
        
       
    
    # Optionally, you can set additional headers with metadata.
        #response.headers["Content-Disposition"] = 'attachment; filename="your_filename.pdf"'
        response = StreamingResponse(io.BytesIO(img.make_blob()), media_type="image/png")
        response.headers["Custom-Metadata"] = "Processing_Units: " + str(pdfReader.numPages )
        return response
        # Return the first page of the PDF as an image
    except Exception as e:
        print (e)
        return {"message": "There was an error processing the PDF file."}

@app.post("/sender_balance")
async def sender_bal(sender_xrp_address: str = Form('rEGugmgEogLNi7ZcCkFAtMNrbqvZ7VAQAE')):
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        response = await asyncio.get_event_loop().run_in_executor(
               executor, get_wallet_balance, sender_xrp_address)
    return response
            
@app.post("/receiver_balance")
async def receiver_bal(receiver_xrp_address: str = Form('rNGukvjVrQhMpFXWMD4Sxvc2mxHMmJtcHJ')):
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        response = await asyncio.get_event_loop().run_in_executor(
               executor, get_wallet_balance, receiver_xrp_address)
    return response

