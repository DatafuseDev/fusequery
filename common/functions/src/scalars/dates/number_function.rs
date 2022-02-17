// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::marker::PhantomData;

use common_datavalues2::chrono::DateTime;
use common_datavalues2::chrono::Datelike;
use common_datavalues2::chrono::TimeZone;
use common_datavalues2::chrono::Timelike;
use common_datavalues2::chrono::Utc;
use common_datavalues2::prelude::*;
use common_datavalues2::Date16Type;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function2_factory::Function2Description;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::CastFunction;
use crate::scalars::Function2;
use crate::scalars::Function2Adapter;
use crate::scalars::Monotonicity2;
use crate::scalars::RoundFunction;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone, Debug)]
pub struct NumberFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

pub trait NumberOperator<R> {
    const IS_DETERMINISTIC: bool;

    fn to_number(_value: DateTime<Utc>) -> R;
    // Used to check the monotonicity of the function.
    // For example, ToDayOfYear is monotonous only when the time range is the same year.
    // So we can use ToStartOfYearFunction to check whether the time range is in the same year.
    // If the function always monotonous, just return error.
    fn factor_function() -> Result<Box<dyn Function2>> {
        Err(ErrorCode::UnknownException(
            "Always monotonous, has no factor function",
        ))
    }

    fn return_type() -> Option<common_datavalues2::DataTypePtr> {
        None
    }
}

#[derive(Clone)]
pub struct ToYYYYMM;

impl NumberOperator<u32> for ToYYYYMM {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u32 {
        value.year() as u32 * 100 + value.month()
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDD;

impl NumberOperator<u32> for ToYYYYMMDD {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u32 {
        value.year() as u32 * 10000 + value.month() * 100 + value.day()
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDDhhmmss;

impl NumberOperator<u64> for ToYYYYMMDDhhmmss {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u64 {
        value.year() as u64 * 10000000000
            + value.month() as u64 * 100000000
            + value.day() as u64 * 1000000
            + value.hour() as u64 * 10000
            + value.minute() as u64 * 100
            + value.second() as u64
    }
}

#[derive(Clone)]
pub struct ToStartOfYear;

impl NumberOperator<u16> for ToStartOfYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        let end: DateTime<Utc> = Utc.ymd(value.year(), 1, 1).and_hms(0, 0, 0);
        get_day(end) as u16
    }

    fn return_type() -> Option<common_datavalues2::DataTypePtr> {
        Some(Date16Type::arc())
    }
}

#[derive(Clone)]
pub struct ToStartOfISOYear;

impl NumberOperator<u16> for ToStartOfISOYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        let week_day = value.weekday().num_days_from_monday();
        let iso_week = value.iso_week();
        let iso_week_num = iso_week.week();
        let sub_days = (iso_week_num - 1) * 7 + week_day;
        let result = value.timestamp_millis() - sub_days as i64 * 24 * 3600 * 1000;
        let end: DateTime<Utc> = Utc.timestamp_millis(result);
        get_day(end) as u16
    }

    fn return_type() -> Option<common_datavalues2::DataTypePtr> {
        Some(Date16Type::arc())
    }
}

#[derive(Clone)]
pub struct ToStartOfQuarter;

impl NumberOperator<u16> for ToStartOfQuarter {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        let new_month = value.month0() / 3 * 3 + 1;
        let date = Utc.ymd(value.year(), new_month, 1).and_hms(0, 0, 0);
        get_day(date) as u16
    }

    fn return_type() -> Option<common_datavalues2::DataTypePtr> {
        Some(Date16Type::arc())
    }
}

#[derive(Clone)]
pub struct ToStartOfMonth;

impl NumberOperator<u16> for ToStartOfMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        let date = Utc.ymd(value.year(), value.month(), 1).and_hms(0, 0, 0);
        get_day(date) as u16
    }

    fn return_type() -> Option<common_datavalues2::DataTypePtr> {
        Some(Date16Type::arc())
    }
}

#[derive(Clone)]
pub struct ToMonth;

impl NumberOperator<u8> for ToMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.month() as u8
    }

    // ToMonth is NOT a monotonic function in general, unless the time range is within the same year.
    // For example, date(2020-12-01) < date(2021-5-5), while ToMonth(2020-12-01) > ToMonth(2021-5-5).
    fn factor_function() -> Result<Box<dyn Function2>> {
        ToStartOfYearFunction::try_create("toStartOfYear")
    }
}

#[derive(Clone)]
pub struct ToDayOfYear;

impl NumberOperator<u16> for ToDayOfYear {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        value.ordinal() as u16
    }

    // ToDayOfYear is NOT a monotonic function in general, unless the time range is within the same year.
    // For example, date(2020-12-01) < date(2021-5-5), while ToDayOfYear(2020-12-01) > ToDayOfYear(2021-5-5).
    fn factor_function() -> Result<Box<dyn Function2>> {
        ToStartOfYearFunction::try_create("toStartOfYear")
    }
}

#[derive(Clone)]
pub struct ToDayOfMonth;

impl NumberOperator<u8> for ToDayOfMonth {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.day() as u8
    }

    // ToDayOfMonth is not a monotonic function in general, unless the time range is within the same month.
    // For example, date(2021-11-20) < date(2021-12-01), while ToDayOfMonth(2021-11-20) > ToDayOfMonth(2021-12-01).
    fn factor_function() -> Result<Box<dyn Function2>> {
        ToStartOfMonthFunction::try_create("toStartOfMonth")
    }
}

#[derive(Clone)]
pub struct ToDayOfWeek;

impl NumberOperator<u8> for ToDayOfWeek {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.weekday().number_from_monday() as u8
    }

    // ToDayOfWeek is NOT a monotonic function in general, unless the time range is within the same week.
    fn factor_function() -> Result<Box<dyn Function2>> {
        ToMondayFunction::try_create("toMonday")
    }
}

#[derive(Clone)]
pub struct ToHour;

impl NumberOperator<u8> for ToHour {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.hour() as u8
    }

    // ToHour is NOT a monotonic function in general, unless the time range is within the same day.
    fn factor_function() -> Result<Box<dyn Function2>> {
        let func2 = CastFunction::create("toDate", Date16Type::arc().name()).unwrap();

        Ok(func2)
    }
}

#[derive(Clone)]
pub struct ToMinute;

impl NumberOperator<u8> for ToMinute {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.minute() as u8
    }

    // ToMinute is NOT a monotonic function in general, unless the time range is within the same hour.
    fn factor_function() -> Result<Box<dyn Function2>> {
        RoundFunction::try_create("toStartOfHour", 60 * 60)
    }
}

#[derive(Clone)]
pub struct ToSecond;

impl NumberOperator<u8> for ToSecond {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u8 {
        value.second() as u8
    }

    // ToSecond is NOT a monotonic function in general, unless the time range is within the same minute.
    fn factor_function() -> Result<Box<dyn Function2>> {
        RoundFunction::try_create("toStartOfMinute", 60)
    }
}

#[derive(Clone)]
pub struct ToMonday;

impl NumberOperator<u16> for ToMonday {
    const IS_DETERMINISTIC: bool = true;

    fn to_number(value: DateTime<Utc>) -> u16 {
        let weekday = value.weekday();
        (get_day(value) - weekday.num_days_from_monday()) as u16
    }
}

impl<T, R> NumberFunction<T, R>
where
    T: NumberOperator<R> + Clone + Sync + Send + 'static,
    R: PrimitiveType + Clone + ToDataType + common_datavalues2::Scalar<RefType<'static> = R>,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(NumberFunction::<T, R> {
            display_name: display_name.to_string(),
            t: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().monotonicity().num_arguments(1);

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }
}

impl<T, R> Function2 for NumberFunction<T, R>
where
    T: NumberOperator<R> + Clone + Sync + Send,
    R: PrimitiveType + Clone + ToDataType,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        match T::return_type() {
            None => Ok(R::to_data_type()),
            Some(v) => Ok(v),
        }
    }

    fn eval(
        &self,
        columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        let type_id = columns[0].field().data_type().data_type_id();

        let number_array= match type_id {
            TypeID::Date16 => {
                let unary = ScalarUnaryExpression::<u16, R, _>::new(|v| {
                    let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                    T::to_number(date_time)
                });
                let col = unary.eval(columns[0].column())?;
                Ok(col.arc())

            },
            TypeID::Date32 => {
                let unary = ScalarUnaryExpression::<i32, R, _>::new(|v| {
                    let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                    T::to_number(date_time)
                });
                let col = unary.eval(columns[0].column())?;
                Ok(col.arc())
            },
            TypeID::DateTime32 => {
                let unary = ScalarUnaryExpression::<u32, R, _>::new(|v| {
                    let date_time = Utc.timestamp(v as i64 , 0_u32);
                    T::to_number(date_time)
                });
                let col = unary.eval(columns[0].column())?;
                Ok(col.arc())

                },
            other => Result::Err(ErrorCode::IllegalDataType(format!(
                "Illegal type {:?} of argument of function {}.Should be a date16/data32 or a dateTime32",
                other,
                self.name()))),
        }?;
        Ok(number_array)
    }

    fn get_monotonicity(&self, args: &[Monotonicity2]) -> Result<Monotonicity2> {
        let func = match T::factor_function() {
            Ok(f) => f,
            // Always monotonous, has no factor function.
            Err(_) => return Ok(Monotonicity2::clone_without_range(&args[0])),
        };

        let func = Function2Adapter::create(func);

        if args[0].left.is_none() || args[0].right.is_none() {
            return Ok(Monotonicity2::default());
        }

        let left_val = func.eval(&[args[0].left.clone().unwrap()], 1)?.get(0);
        let right_val = func.eval(&[args[0].right.clone().unwrap()], 1)?.get(0);
        // The function is monotonous, if the factor eval returns the same values for them.
        if left_val == right_val {
            return Ok(Monotonicity2::clone_without_range(&args[0]));
        }

        Ok(Monotonicity2::default())
    }
}

impl<T, R> fmt::Display for NumberFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

fn get_day(date: DateTime<Utc>) -> u32 {
    let start: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
    let duration = date.signed_duration_since(start);
    duration.num_days() as u32
}

pub type ToYYYYMMFunction = NumberFunction<ToYYYYMM, u32>;
pub type ToYYYYMMDDFunction = NumberFunction<ToYYYYMMDD, u32>;
pub type ToYYYYMMDDhhmmssFunction = NumberFunction<ToYYYYMMDDhhmmss, u64>;

pub type ToStartOfISOYearFunction = NumberFunction<ToStartOfISOYear, u16>;
pub type ToStartOfYearFunction = NumberFunction<ToStartOfYear, u16>;
pub type ToStartOfQuarterFunction = NumberFunction<ToStartOfQuarter, u16>;
pub type ToStartOfMonthFunction = NumberFunction<ToStartOfMonth, u16>;

pub type ToMonthFunction = NumberFunction<ToMonth, u8>;
pub type ToDayOfYearFunction = NumberFunction<ToDayOfYear, u16>;
pub type ToDayOfMonthFunction = NumberFunction<ToDayOfMonth, u8>;
pub type ToDayOfWeekFunction = NumberFunction<ToDayOfWeek, u8>;

pub type ToHourFunction = NumberFunction<ToHour, u8>;
pub type ToMinuteFunction = NumberFunction<ToMinute, u8>;
pub type ToSecondFunction = NumberFunction<ToSecond, u8>;

pub type ToMondayFunction = NumberFunction<ToMonday, u16>;
