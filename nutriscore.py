# nutriscore.py
import google.generativeai as genai
import time

class NutriScoreEstimator:
    def __init__(self, api_key):
        """
        Initialize the Gemini API connection.
        """
        if not api_key:
            raise ValueError("API Key must be provided.")
        
        genai.configure(api_key=api_key)
        # Using the flash model for speed and efficiency
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def estimate_score(self, item_name, category, sub_category=""):
        """
        Sends item details to Gemini to estimate a Nutri-Score (A-E).
        """
        prompt = f"""
        You are a nutritionist. Classify the following cafeteria item into a Nutri-Score (A, B, C, D, or E).
        
        Item: "{item_name}"
        Category: "{category}" ({sub_category})
        
        Rules:
        - Nutri-Score A: Highest nutritional quality (Water, Fruits, Vegetables, Unprocessed lean protein).
        - Nutri-Score E: Lowest nutritional quality (Sugary drinks, Candy, Deep fried foods).
        - If it is Water or Unsweetened Tea/Coffee, score it A.
        - If it is a Soda or Energy Drink, score it E.
        - For ambiguous items, estimate based on typical ingredients.
        
        Return ONLY the single letter (A, B, C, D, or E). Do not write any other text.
        """
        
        try:
            # Generate content
            response = self.model.generate_content(prompt)
            score = response.text.strip().upper()
            
            # Validation: Ensure we got a single letter
            if score in ['A', 'B', 'C', 'D', 'E']:
                return score
            else:
                return 'C' # Default to Average if output is ambiguous
                
        except Exception as e:
            print(f"Error scoring {item_name}: {e}")
            # Simple rate limit handling: wait and retry once if needed
            time.sleep(1) 
            return 'C' # Fallback