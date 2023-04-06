//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_exception::ErrorCode;
use common_exception::Result;
use openai_api_rust::completions::CompletionsApi;
use openai_api_rust::completions::CompletionsBody;
use openai_api_rust::Auth;

use crate::metrics::metrics_completion_count;
use crate::metrics::metrics_completion_token;
use crate::OpenAI;

pub enum CompletionMode {
    // SQL translate:
    // max_tokens: 150, stop: ['#', ';']
    SQL,
    // Text completion:
    // max_tokens: 512, stop: none
    Text,
}

impl OpenAI {
    pub fn completion_request(
        &self,
        prompt: String,
        mode: CompletionMode,
    ) -> Result<(String, Option<u32>)> {
        let openai = openai_api_rust::OpenAI::new(
            Auth {
                api_key: self.api_key.clone(),
                organization: None,
            },
            &self.api_base,
        );

        let (max_tokens, stop) = match mode {
            CompletionMode::SQL => (Some(150), Some(vec!["#".to_string(), ";".to_string()])),
            CompletionMode::Text => (Some(512), None),
        };

        let body = CompletionsBody {
            model: self.model.to_string(),
            prompt: Some(vec![prompt]),
            suffix: None,
            max_tokens,
            temperature: Some(0_f32),
            top_p: Some(1_f32),
            n: Some(2),
            stream: Some(false),
            logprobs: None,
            echo: None,
            stop,
            presence_penalty: None,
            frequency_penalty: None,
            best_of: None,
            logit_bias: None,
            user: None,
        };
        let resp = openai.completion_create(&body).map_err(|e| {
            ErrorCode::Internal(format!("openai completion request error: {:?}", e))
        })?;

        let usage = resp.usage.total_tokens;
        let sql = if resp.choices.is_empty() {
            "".to_string()
        } else {
            resp.choices[0].text.clone().unwrap_or("".to_string())
        };

        // perf.
        {
            metrics_completion_count(1);
            metrics_completion_token(usage.unwrap_or(0));
        }

        Ok((sql, usage))
    }
}
