import { useState } from "react";
import ReactModal from "react-modal";
import SchemaImg from "./assets/schema.png";

const schema = {
  patients: [
    { name: "patient_id", type: "INT", key: "primary" },
    { name: "first_name", type: "TEXT" },
    { name: "last_name", type: "TEXT" },
    { name: "gender", type: "CHAR(1)" },
    { name: "birth_date", type: "DATE" },
    { name: "city", type: "TEXT" },
    { name: "province_id", type: "CHAR(2)", key: "foreign" },
    { name: "allergies", type: "TEXT" },
    { name: "height", type: "INT" },
    { name: "weight", type: "INT" },
  ],
  admissions: [
    { name: "patient_id", type: "INT", key: "foreign" },
    { name: "admission_date", type: "DATE" },
    { name: "discharge_date", type: "DATE" },
    { name: "diagnosis", type: "TEXT" },
    { name: "attending_doctor_id", type: "INT", key: "foreign" },
  ],
  doctors: [
    { name: "doctor_id", type: "INT", key: "primary" },
    { name: "first_name", type: "TEXT" },
    { name: "last_name", type: "TEXT" },
    { name: "specialty", type: "TEXT" },
  ],
  province_names: [
    { name: "province_id", type: "CHAR(2)", key: "primary" },
    { name: "province_name", type: "TEXT" },
  ],
};

const KeyIcon = ({ type }) => {
  if (!type) return null;
  return (
    <span className="mr-2 text-gray-500">
      {type === "primary" ? "🔑" : "🗝️"}
    </span>
  );
};

const TableBlock = ({ name, columns, isOpen, onClick }) => {
  return (
    <div className="mb-2 rounded-lg border border-gray-200 bg-white">
      {/* Header */}
      <button
        onClick={onClick}
        className="w-full flex items-center justify-between px-3 py-1 text-sm font-semibold text-gray-700 bg-gray-50 border-b hover:bg-gray-100"
      >
        {name}
        <span className="text-gray-400">
          {isOpen ? "▾" : "▸"}
        </span>
      </button>

      {/* Content */}
      {isOpen && (
        <div className="divide-y">
          {columns.length > 0 ? (
            columns.map((col, index) => (
              <div
                key={index}
                className="flex items-center justify-between px-3 py-1 text-sm"
              >
                <div className="flex items-center text-gray-700">
                  <KeyIcon type={col.key} />
                  {col.name}
                </div>
                <div className="text-gray-500 font-mono">
                  {col.type}
                </div>
              </div>
            ))
          ) : (
            <div className="px-3 py-1 text-sm text-gray-400 italic">
              No columns
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const SchemaViewer = () => {
  const [openTable, setOpenTable] = useState("patients");
  const [show, setShow] = useState(false);

  const handleToggle = (tableName) => {
    setOpenTable((prev) =>
      prev === tableName ? null : tableName
    );
  };

  return (
    <div className="w-80 bg-gray-100 p-3">
      <button className="text-md font-semibold mb-3 text-align cursor-pointer border-1 py-1 px-2 rounded-lg" onClick={() => setShow(!show)}>View Schema</button>
      {Object.entries(schema).map(([tableName, columns]) => (
        <TableBlock
          key={tableName}
          name={tableName}
          columns={columns}
          isOpen={openTable === tableName}
          onClick={() => handleToggle(tableName)}
        />
      ))}
      <ReactModal
        isOpen={show}
        onRequestClose={() => setShow(false)}
        contentLabel="Schema Modal"
        className="max-w-3xl mx-auto my-20 p-6 rounded-lg shadow-lg outline-none"
        overlayClassName="fixed inset-0 bg-[#ffffffb3] flex items-center justify-center"
      >
        <img src={SchemaImg} alt="Database Schema" className="w-full h-auto" />
        </ReactModal>
    </div>
  );
};

export default SchemaViewer;
