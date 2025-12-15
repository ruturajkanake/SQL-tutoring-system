import React, { useState, useEffect, use } from "react";
import Editor from "@monaco-editor/react";

import questions from "../../questions.json";
import SchemaViewer from "./Schema";

const DynamicTable = ({ data }) => {
  const { cols, rows } = data;

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 pb-5">
      <table className="min-w-full border-collapse">
        <thead className="bg-gray-100">
          <tr>
            {cols.map((col, index) => (
              <th
                key={index}
                className="px-4 py-2 text-left text-sm font-semibold text-gray-700 border-b"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {rows.map((row, rowIndex) => (
            <tr
              key={rowIndex}
              className="hover:bg-gray-50 transition-colors"
            >
              {row.map((cell, cellIndex) => (
                <td
                  key={cellIndex}
                  className="px-4 py-2 text-sm text-gray-800 border-b"
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};


export default function App() {

  const [sql, setSql] = useState("SELECT first_name, last_name FROM patients;");
  const [output, setOutput] = useState(null);
  const [hint, setHint] = useState(null);
  const [id, setId] = useState(1);
  const [hint_level, setHintLevel] = useState(1);
  const [toggle, setToggle] = useState(false);

  // API endpoints
  const API_VALIDATE = "http://localhost:8000/validate";
  const API_HINT = "http://localhost:8000/hint";

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const qid = parseInt(params.get('id')) || 1;
    setId(qid);
  }, []);

  const runQuery = async () => {
    setHint(null);
    const res = await fetch(API_VALIDATE, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        student_sql: sql,
        question_number: id
      })
    });

    const data = await res.json();
    setOutput(data);
    
  };

  const getHint = async () => {
    const res = await fetch(API_HINT, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        student_sql: sql,
        question_number: id,
        hint_level: hint_level
      })
    });

    const data = await res.json();
    setHint(data.hint);
    let newData = {...data};
    newData["student_output"] = data["execution"]["student"];
    setOutput(newData)
  };

  return (
    <div className="">
      {/* MAIN CONTENT */}
      <div className="flex h-[500px]">
        <div className="w-full">
          <div className="p-4">Question Number {id}</div>
          {/* SQL EDITOR SECTION */}
          <div className="p-4">
            <Editor
              height="100px"
              width="100%"
              defaultLanguage="sql"
              value={sql}
              onChange={(val) => setSql(val)}
              theme="vs-light"
              className="border"
            />
          </div>

          {/* RIGHT PANEL */}
          <div className="p-4 bg-white border-l flex flex-col">
        

            {/* Question Box */}
            <div className="text-gray-700 font-medium mb-4">
              {questions[id-1].question}
            </div>
            {/* Solution Box */}
            <div className="border rounded p-3 bg-gray-50 overflow-auto">
              <div className="text-gray-500 text-sm cursor-pointer" onClick={() => setToggle(!toggle)}>View Solution</div>
              {toggle && <pre className="text-sm mt-2 text-blue-600">{questions[id-1].answer_ref}</pre>}
            </div>

            <div className="flex gap-2 mt-4">
              <button 
                onClick={runQuery}
                className="px-4 py-1 rounded bg-blue-600 text-white"
              >
                Run
              </button>
              <div className="flex items-center gap-2">
                <div className="flex-1">Hint Level</div>
                <select
                  value={hint_level}
                  onChange={(e) => setHintLevel(parseInt(e.target.value))}
                  className="border rounded px-2 py-1"
                >
                  <option value={1}>1</option>
                  <option value={2}>2</option>
                  <option value={3}>3 (Language Agnostic)</option>
                  <option value={4}>4 (LLM)</option>
                  <option value={5}>5 (LLM)</option>
                </select>
                <button 
                  onClick={getHint}
                  className="px-4 py-1 rounded bg-green-600 text-white"
                >
                  Get Hint
                </button>
              </div>
            </div>

            {hint && (
              <div className="mt-4 p-3 bg-yellow-100 border rounded text-gray-800">
                <strong>Hint: </strong>{hint}
              </div>
            )}
          </div>
        </div>
        <SchemaViewer />
      </div>
      
      {/*Previous and next question*/}
      <div className="p-3 bg-gray-200 flex justify-between">
        <button 
          onClick={() => setId(Math.max(1, id - 1))}
          className="px-4 py-1 rounded bg-gray-600 text-white"
        >
          Previous Question
        </button>
        <button 
          onClick={() => setId(Math.min(51, id + 1))}
          className="px-4 py-1 rounded bg-gray-600 text-white"
        >
          Next Question
        </button>
      </div>

      {/* OUTPUT SECTION */}
      <div className="h-40 bg-white border-t p-3">
        <div className="font-semibold mb-1">Practice SQL</div>

        {output && output["success"] ? (
          <div className="text-green-600 font-medium">
            Success! Your query is correct.
          </div>) : (
          <div className="text-red-600 font-medium">
            {output ? `Wrong answer` : "Wrong answer"}
          </div>)
        }
        <div className="text-gray-600 mb-2 text-sm">
          Query results are output here.
        </div>
        {output && output["student_output"] && (
          <DynamicTable data={output["student_output"]} />
        )}
      </div>
    </div>
  );
}
