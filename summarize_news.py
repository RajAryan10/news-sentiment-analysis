from transformers import BartTokenizer, BartForConditionalGeneration, pipeline
import torch
import os
import logging


logging.getLogger("transformers").setLevel(logging.ERROR)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
logger = logging.getLogger(__name__)

class BartSummarizer:
    def __init__(self, max_length):
        self.max_length = max_length 
        self.model_name = 'facebook/bart-large-cnn'
        self.model = None
        self.tokenizer = None
        self.load_model()

    def load_model(self):
        if self.model is None or self.tokenizer is None:
            logger.info(" Loading Model....")
            self.tokenizer = BartTokenizer.from_pretrained(self.model_name)
            self.model = BartForConditionalGeneration.from_pretrained(self.model_name)
            logger.info(" Model Loaded ....")

    def summarize(self, prompt):
        inputs = self.tokenizer.encode(prompt, return_tensors="pt", max_length=1024, truncation=True)
        summary_ids = self.model.generate(inputs, max_length=self.max_length)
        summary = self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        return summary

if __name__ == '__main__':
    news_content = """Lionel Andrés "Leo" Messi[note 1] (Spanish pronunciation: [ljoˈnel anˈdɾes ˈmesi] ⓘ; born 24 June 1987) is an Argentine professional footballer who plays as a forward for and captains both Major League Soccer club Inter Miami and the Argentina national team. Widely regarded as one of the greatest players of all time, Messi has won a record eight Ballon d'Or awards, a record six European Golden Shoes, and was named the world's best player for a record eight times by FIFA.[note 2] Messi is the most decorated player in the history of professional football, with 44 team trophies.[note 3] Until 2021, he had spent his entire professional career with Barcelona, where he won a club-record 34 trophies, including ten La Liga titles, seven Copa del Rey titles, and the UEFA Champions League four times. Playing for Argentina's national team, he won two Copa América titles and the 2022 FIFA World Cup. A prolific goalscorer and creative playmaker, Messi holds the records for most goals (474), hat-tricks (36), and assists (192) in La Liga, most appearances (39) and assists (18) in the Copa América. He has the most international goals (109) and appearances (187) by a South American male. Messi has scored over 800 senior career goals for club and country, and the most goals for a single club (672).
Messi relocated to Spain and joined Barcelona aged 13, making his competitive debut at age 17 in October 2004. He established himself as an integral player for the club within the next three years, and in his first uninterrupted season in 2008–09 helped Barcelona achieve the first treble in Spanish football; that year, aged 22, Messi won his first Ballon d'Or. Messi won four consecutive Ballons d'Or, the first player to win it four times. During the 2011–12 season, he set La Liga and European records for most goals in a season, while establishing himself as Barcelona's all-time top scorer. The following two seasons, he finished second for the Ballon d'Or behind Cristiano Ronaldo, his perceived career rival, before regaining his best form during the 2014–15 campaign, becoming the all-time top scorer in La Liga and leading Barcelona to a historic second treble, and was awarded a fifth Ballon d'Or in 2015. Messi assumed captaincy of Barcelona in 2018, and won a record sixth Ballon d'Or in 2019. He signed for French club Paris Saint-Germain in August 2021, spending two seasons there and winning Ligue 1 twice. Messi joined American club Inter Miami in July 2023, winning the Leagues Cup in August.
An Argentine international, Messi is the country's all-time leading goalscorer and holds the national record for appearances. At youth level, he won the 2005 FIFA World Youth Championship and gold at the 2008 Summer Olympics. His style of play as a diminutive, left-footed dribbler drew comparisons with compatriot Diego Maradona, who described Messi as his successor. After his senior debut in 2005, Messi became the youngest Argentine to play and score in a FIFA World Cup (2006). As the squad's captain from 2011, he led Argentina to three consecutive finals: the 2014 FIFA World Cup, the 2015 Copa América and the 2016 Copa América. After announcing his international retirement in 2016, he returned to lead his country to qualification for the 2018 FIFA World Cup and victory in the 2021 Copa América. He led Argentina to win the 2022 World Cup, where he won a record second Golden Ball, scored seven goals including two in the final, and broke the record for most games played at the World Cup (26), later receiving his record-extending eighth Ballon d'Or in 2023. He then won a second Copa América as captain in 2024.
Messi has endorsed sportswear company Adidas since 2006. According to France Football, he was the world's highest-paid footballer for five years out of six between 2009 and 2014, and was ranked the world's highest-paid athlete by Forbes in 2019 and 2022. Messi was among Time's 100 most influential people in the world in 2011, 2012, and 2023. In 2020 and 2023, he was named the Laureus World Sportsman of the Year, the first team-sport athlete to win it. In 2020, Messi was named to the Ballon d'Or Dream Team and became the second footballer and second team-sport athlete to surpass $1 billion in career earnings. In 2024, Messi founded Más+.
    """
    prompt = f"Summarize the given text: {news_content}"

    summarizer = BartSummarizer(prompt, max_length=250)
    summary = summarizer.summarize()
    print(f"Summary: {summary}")
    
