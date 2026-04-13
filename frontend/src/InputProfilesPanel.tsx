import { useEffect, useMemo, useState } from "react";
import {
  createProfile,
  deleteProfile,
  getProfile,
  listProfiles,
  saveProfileCoefficients,
  saveProfileAs,
  saveProfileMLSettings,
  saveProfileOutputs,
  saveProfileParameters,
  updateProfileMeta,
  type CoefficientProfileRow,
  type InputProfileDetail,
  type InputProfileMLSettings,
  type InputProfileSummary,
  type OutputProfileRow,
  type ParameterProfileRow,
} from "./api";

type ProfileTab = "parameters" | "coefficients" | "outputs" | "ml";
type ProfilePanelMode = "summary" | "editor";

type Props = {
  ifsRoot: string | null;
  outputDirectory: string | null;
  ifsStaticId: number | null | undefined;
  selectedProfileId: number | null;
  onSelectedProfileIdChange: (profileId: number | null) => void;
  onProfileDetailChange: (detail: InputProfileDetail | null) => void;
  onEditorActiveChange?: (active: boolean) => void;
};

const PROFILE_TABS: ProfileTab[] = ["parameters", "coefficients", "outputs", "ml"];
const DEFAULT_ML_SETTINGS: InputProfileMLSettings = {
  ml_method: "",
  fit_metric: "mse",
  n_sample: 200,
  n_max_iteration: 30,
  n_convergence: 10,
  min_convergence_pct: 0.0001,
};

function normalizeSearch(value: string): string {
  return value.trim().toLowerCase();
}

function nextTab(tab: ProfileTab): ProfileTab | null {
  const index = PROFILE_TABS.indexOf(tab);
  return index >= 0 && index < PROFILE_TABS.length - 1 ? PROFILE_TABS[index + 1] : null;
}

function previousTab(tab: ProfileTab): ProfileTab | null {
  const index = PROFILE_TABS.indexOf(tab);
  return index > 0 ? PROFILE_TABS[index - 1] : null;
}

function ValidationBadge({ valid }: { valid: boolean }) {
  return (
    <span className={`status ${valid ? "success" : "incomplete"}`}>
      {valid ? "Valid" : "Incomplete"}
    </span>
  );
}

function SummaryCard({
  title,
  value,
  detail,
}: {
  title: string;
  value: string | number;
  detail?: string | null;
}) {
  return (
    <div className="profile-summary-card">
      <span className="profile-summary-card-label">{title}</span>
      <strong className="profile-summary-card-value">{value}</strong>
      {detail ? <span className="profile-summary-card-detail">{detail}</span> : null}
    </div>
  );
}

function isIntermediateNumericDraft(value: string): boolean {
  const trimmed = value.trim();
  return trimmed === "-" || trimmed === "." || trimmed === "-.";
}

function DraftNumberInput({
  value,
  onValueChange,
  integer = false,
  className = "profile-number-input",
}: {
  value: number | null | undefined;
  onValueChange: (value: number | null) => void;
  integer?: boolean;
  className?: string;
}) {
  const [draft, setDraft] = useState(value == null ? "" : String(value));
  const [focused, setFocused] = useState(false);

  useEffect(() => {
    if (!focused) {
      setDraft(value == null ? "" : String(value));
    }
  }, [focused, value]);

  const commitDraft = (nextDraft: string) => {
    const trimmed = nextDraft.trim();
    if (!trimmed) {
      onValueChange(null);
      return "";
    }
    if (isIntermediateNumericDraft(trimmed)) {
      return value == null ? "" : String(value);
    }
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) {
      return value == null ? "" : String(value);
    }
    if (integer && !Number.isInteger(parsed)) {
      return value == null ? "" : String(value);
    }
    const normalized = integer ? Math.trunc(parsed) : parsed;
    onValueChange(normalized);
    return String(normalized);
  };

  return (
    <input
      type="text"
      inputMode={integer ? "numeric" : "decimal"}
      className={className}
      value={draft}
      onFocus={() => setFocused(true)}
      onChange={(event) => {
        const nextDraft = event.target.value;
        setDraft(nextDraft);
        const trimmed = nextDraft.trim();
        if (!trimmed) {
          onValueChange(null);
          return;
        }
        if (isIntermediateNumericDraft(trimmed)) {
          return;
        }
        const parsed = Number(trimmed);
        if (!Number.isFinite(parsed)) {
          return;
        }
        if (integer && !Number.isInteger(parsed)) {
          return;
        }
        onValueChange(integer ? Math.trunc(parsed) : parsed);
      }}
      onBlur={(event) => {
        setFocused(false);
        setDraft(commitDraft(event.target.value));
      }}
    />
  );
}

function formatSummaryValue(value: number | null | undefined): string {
  return value == null ? "Not set" : String(value);
}

function buildRangeSummary({
  minimum,
  maximum,
  step,
  levelCount,
}: {
  minimum: number | null | undefined;
  maximum: number | null | undefined;
  step: number | null | undefined;
  levelCount: number | null | undefined;
}): string {
  return [
    `Min ${formatSummaryValue(minimum)}`,
    `Max ${formatSummaryValue(maximum)}`,
    `Step ${formatSummaryValue(step)}`,
    `Levels ${formatSummaryValue(levelCount)}`,
  ].join(" | ");
}

function buildSelectedSummary({
  defaultValue,
  minimum,
  maximum,
  step,
  levelCount,
}: {
  defaultValue: number | null | undefined;
  minimum: number | null | undefined;
  maximum: number | null | undefined;
  step: number | null | undefined;
  levelCount: number | null | undefined;
}): string {
  return [
    `Default ${formatSummaryValue(defaultValue)}`,
    `Min ${formatSummaryValue(minimum)}`,
    `Max ${formatSummaryValue(maximum)}`,
    `Step ${formatSummaryValue(step)}`,
    `Levels ${formatSummaryValue(levelCount)}`,
  ].join(" | ");
}

export function InputProfilesPanel({
  ifsRoot,
  outputDirectory,
  ifsStaticId,
  selectedProfileId,
  onSelectedProfileIdChange,
  onProfileDetailChange,
  onEditorActiveChange,
}: Props) {
  const [profiles, setProfiles] = useState<InputProfileSummary[]>([]);
  const [detail, setDetail] = useState<InputProfileDetail | null>(null);
  const [panelMode, setPanelMode] = useState<ProfilePanelMode>("summary");
  const [activeTab, setActiveTab] = useState<ProfileTab>("parameters");
  const [busy, setBusy] = useState(false);
  const [loadingProfiles, setLoadingProfiles] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isSaveAsModalOpen, setIsSaveAsModalOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createDescription, setCreateDescription] = useState("");
  const [saveAsName, setSaveAsName] = useState("");
  const [saveAsDescription, setSaveAsDescription] = useState("");
  const [metaName, setMetaName] = useState("");
  const [metaDescription, setMetaDescription] = useState("");
  const [parameterRows, setParameterRows] = useState<ParameterProfileRow[]>([]);
  const [coefficientRows, setCoefficientRows] = useState<CoefficientProfileRow[]>([]);
  const [outputRows, setOutputRows] = useState<OutputProfileRow[]>([]);
  const [mlSettings, setMLSettings] = useState<InputProfileMLSettings | null>(null);
  const [parameterSearch, setParameterSearch] = useState("");
  const [coefficientSearch, setCoefficientSearch] = useState("");
  const [outputSearch, setOutputSearch] = useState("");
  const [showAllParameters, setShowAllParameters] = useState(false);
  const [showAllCoefficients, setShowAllCoefficients] = useState(false);
  const [showAllOutputs, setShowAllOutputs] = useState(false);
  const [expandedParameterKey, setExpandedParameterKey] = useState<string | null>(null);
  const [expandedCoefficientKey, setExpandedCoefficientKey] = useState<string | null>(null);
  const [parameterDraft, setParameterDraft] = useState<ParameterProfileRow | null>(null);
  const [coefficientDraft, setCoefficientDraft] = useState<CoefficientProfileRow | null>(null);

  const canLoadProfiles = Boolean(outputDirectory && ifsStaticId && ifsStaticId > 0);

  useEffect(() => {
    onEditorActiveChange?.(panelMode === "editor");
  }, [panelMode, onEditorActiveChange]);

  const applyDetail = (nextDetail: InputProfileDetail | null) => {
    setDetail(nextDetail);
    if (!nextDetail) {
      setMetaName("");
      setMetaDescription("");
      setParameterRows([]);
      setCoefficientRows([]);
      setOutputRows([]);
      setMLSettings(null);
      setExpandedParameterKey(null);
      setExpandedCoefficientKey(null);
      setParameterDraft(null);
      setCoefficientDraft(null);
      onProfileDetailChange(null);
      return;
    }
    setMetaName(nextDetail.profile.name);
    setMetaDescription(nextDetail.profile.description ?? "");
    setParameterRows(nextDetail.parameter_catalog);
    setCoefficientRows(nextDetail.coefficient_catalog);
    setOutputRows(nextDetail.output_catalog);
    setMLSettings(nextDetail.ml_settings);
    onProfileDetailChange(nextDetail);
  };

  const refreshProfiles = async (preferredProfileId?: number | null) => {
    if (!canLoadProfiles || !outputDirectory || !ifsStaticId) {
      setProfiles([]);
      applyDetail(null);
      setPanelMode("summary");
      return;
    }
    setLoadingProfiles(true);
    try {
      const nextProfiles = await listProfiles(outputDirectory, ifsStaticId);
      setProfiles(nextProfiles);
      const nextSelectedId =
        preferredProfileId != null &&
        nextProfiles.some((profile) => profile.profile_id === preferredProfileId)
          ? preferredProfileId
          : selectedProfileId != null &&
              nextProfiles.some((profile) => profile.profile_id === selectedProfileId)
            ? selectedProfileId
            : null;
      onSelectedProfileIdChange(nextSelectedId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load saved profiles.");
    } finally {
      setLoadingProfiles(false);
    }
  };

  const loadProfileDetail = async (profileId: number | null) => {
    if (!outputDirectory || !profileId) {
      applyDetail(null);
      return null;
    }
    setLoadingDetail(true);
    try {
      const nextDetail = await getProfile(outputDirectory, profileId, ifsRoot);
      applyDetail(nextDetail);
      return nextDetail;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load profile details.");
      applyDetail(null);
      return null;
    } finally {
      setLoadingDetail(false);
    }
  };

  useEffect(() => {
    void refreshProfiles();
  }, [canLoadProfiles, outputDirectory, ifsStaticId]);

  useEffect(() => {
    if (!selectedProfileId) {
      applyDetail(null);
      setPanelMode("summary");
      return;
    }
    void loadProfileDetail(selectedProfileId);
  }, [outputDirectory, selectedProfileId, ifsRoot]);

  useEffect(() => {
    setExpandedParameterKey(null);
    setExpandedCoefficientKey(null);
    setParameterDraft(null);
    setCoefficientDraft(null);
  }, [selectedProfileId]);

  const summaryText = useMemo(() => {
    if (!detail) {
      return "Create or select an input profile to prepare your tuning setup.";
    }
    if (
      detail.validation.enabled_param_count === 0 &&
      detail.validation.enabled_coefficient_count === 0 &&
      detail.validation.enabled_output_count === 0
    ) {
      return "This blank profile is ready to edit. Start with parameters, then enable at least one output.";
    }
    return `${detail.validation.enabled_param_count} parameters, ${detail.validation.enabled_coefficient_count} coefficients, and ${detail.validation.enabled_output_count} outputs are enabled.`;
  }, [detail]);

  const isBlankProfile =
    detail != null &&
    detail.validation.enabled_param_count === 0 &&
    detail.validation.enabled_coefficient_count === 0 &&
    detail.validation.enabled_output_count === 0;

  const selectedParameters = useMemo(() => parameterRows.filter((row) => row.enabled), [parameterRows]);
  const selectedCoefficients = useMemo(() => coefficientRows.filter((row) => row.enabled), [coefficientRows]);
  const selectedOutputs = useMemo(() => outputRows.filter((row) => row.enabled), [outputRows]);

  const filteredParameterCatalog = useMemo(() => {
    const search = normalizeSearch(parameterSearch);
    return parameterRows.filter((row) => {
      if (row.enabled || (!search && !showAllParameters)) {
        return false;
      }
      return !search || normalizeSearch(row.param_name).includes(search);
    });
  }, [parameterRows, parameterSearch, showAllParameters]);

  const filteredCoefficientCatalog = useMemo(() => {
    const search = normalizeSearch(coefficientSearch);
    return coefficientRows.filter((row) => {
      if (row.enabled || (!search && !showAllCoefficients)) {
        return false;
      }
      const haystack = normalizeSearch(
        `${row.function_name} ${row.x_name} ${row.beta_name} ${row.y_name ?? ""}`,
      );
      return !search || haystack.includes(search);
    });
  }, [coefficientRows, coefficientSearch, showAllCoefficients]);

  const filteredOutputCatalog = useMemo(() => {
    const search = normalizeSearch(outputSearch);
    return outputRows.filter((row) => {
      if (row.enabled || (!search && !showAllOutputs)) {
        return false;
      }
      const haystack = normalizeSearch(`${row.variable} ${row.table_name}`);
      return !search || haystack.includes(search);
    });
  }, [outputRows, outputSearch, showAllOutputs]);

  const openCreateModal = () => {
    setError(null);
    setMessage(null);
    setCreateName("");
    setCreateDescription("");
    setIsCreateModalOpen(true);
  };

  const closeCreateModal = () => {
    setError(null);
    setIsCreateModalOpen(false);
    setCreateName("");
    setCreateDescription("");
  };

  const openSaveAsModal = () => {
    if (!detail) {
      return;
    }
    setError(null);
    setMessage(null);
    setSaveAsName(`${detail.profile.name} Copy`);
    setSaveAsDescription(detail.profile.description ?? "");
    setIsSaveAsModalOpen(true);
  };

  const closeSaveAsModal = () => {
    setError(null);
    setIsSaveAsModalOpen(false);
    setSaveAsName("");
    setSaveAsDescription("");
  };

  const handleCreate = async () => {
    if (!outputDirectory || !ifsStaticId) {
      setError("Validate an IFs folder and output folder first.");
      return;
    }
    if (!createName.trim()) {
      setError("Enter a profile name first.");
      return;
    }
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const created = await createProfile(
        outputDirectory,
        ifsStaticId,
        createName.trim(),
        createDescription.trim() || null,
      );
      applyDetail(created);
      onSelectedProfileIdChange(created.profile.profile_id);
      setActiveTab("parameters");
      setPanelMode("editor");
      closeCreateModal();
      await refreshProfiles(created.profile.profile_id);
      setMessage("Profile created. Start with parameters.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to create the input profile.");
    } finally {
      setBusy(false);
    }
  };

  const handleOpenEditor = async () => {
    if (!selectedProfileId) {
      setError("Select a profile to open in the editor.");
      return;
    }
    setError(null);
    setMessage(null);
    const nextDetail =
      detail && detail.profile.profile_id === selectedProfileId
        ? detail
        : await loadProfileDetail(selectedProfileId);
    if (!nextDetail) {
      return;
    }
    setPanelMode("editor");
  };

  const handleEditorBack = () => {
    setPanelMode("summary");
    setMessage(null);
    setError(null);
  };

  const saveDetail = async (
    work: () => Promise<InputProfileDetail>,
    successMessage: string,
  ) => {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const updated = await work();
      applyDetail(updated);
      setMessage(successMessage);
      return updated;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Profile request failed.");
      return null;
    } finally {
      setBusy(false);
    }
  };

  const handleSaveMeta = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    const updated = await saveDetail(
      () => updateProfileMeta(outputDirectory, detail.profile.profile_id, metaName, metaDescription),
      "Profile details saved.",
    );
    if (updated) {
      await refreshProfiles(updated.profile.profile_id);
    }
  };

  const handleSaveAs = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    if (!saveAsName.trim()) {
      setError("Enter a name for the new profile first.");
      return;
    }
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      let copied = await saveProfileAs(outputDirectory, detail.profile.profile_id, saveAsName.trim());
      const normalizedDescription = saveAsDescription.trim();
      const copiedDescription = copied.profile.description ?? "";
      if (normalizedDescription !== copiedDescription) {
        copied = await updateProfileMeta(
          outputDirectory,
          copied.profile.profile_id,
          copied.profile.name,
          normalizedDescription || null,
        );
      }
      applyDetail(copied);
      onSelectedProfileIdChange(copied.profile.profile_id);
      closeSaveAsModal();
      await refreshProfiles(copied.profile.profile_id);
      setMessage("Profile saved as a new copy.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save the profile as a new copy.");
    } finally {
      setBusy(false);
    }
  };

  const handleDelete = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    if (!window.confirm(`Delete profile "${detail.profile.name}"? This cannot be undone.`)) {
      return;
    }
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await deleteProfile(outputDirectory, detail.profile.profile_id);
      applyDetail(null);
      onSelectedProfileIdChange(null);
      setPanelMode("summary");
      await refreshProfiles(null);
      setMessage("Profile deleted.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to delete the profile.");
    } finally {
      setBusy(false);
    }
  };

  const saveParameters = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    await saveDetail(
      () => saveProfileParameters(outputDirectory, detail.profile.profile_id, parameterRows),
      "Parameters saved.",
    );
  };

  const saveCoefficients = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    await saveDetail(
      () => saveProfileCoefficients(outputDirectory, detail.profile.profile_id, coefficientRows),
      "Coefficients saved.",
    );
  };

  const saveOutputs = async () => {
    if (!outputDirectory || !detail) {
      return;
    }
    await saveDetail(
      () => saveProfileOutputs(outputDirectory, detail.profile.profile_id, outputRows),
      "Outputs saved.",
    );
  };

  const saveMlSettings = async () => {
    if (!outputDirectory || !detail || !mlSettings) {
      return;
    }
    await saveDetail(
      () => saveProfileMLSettings(outputDirectory, detail.profile.profile_id, mlSettings),
      "ML settings saved.",
    );
  };

  const renderCatalogEmptyState = (label: string) => (
    <div className="profile-catalog-empty">
      Search or expand the catalog to add more {label}.
    </div>
  );

  const updateParameterRow = (
    paramName: string,
    updater: (row: ParameterProfileRow) => ParameterProfileRow,
  ) => {
    setParameterRows((current) =>
      current.map((item) => (item.param_name === paramName ? updater(item) : item)),
    );
  };

  const updateCoefficientRow = (
    rowKey: string,
    updater: (row: CoefficientProfileRow) => CoefficientProfileRow,
  ) => {
    setCoefficientRows((current) =>
      current.map((item) =>
        `${item.function_name}-${item.x_name}-${item.beta_name}` === rowKey ? updater(item) : item,
      ),
    );
  };

  const updateParameterDraft = (updater: (row: ParameterProfileRow) => ParameterProfileRow) => {
    setParameterDraft((current) => (current ? updater(current) : current));
  };

  const updateCoefficientDraft = (
    updater: (row: CoefficientProfileRow) => CoefficientProfileRow,
  ) => {
    setCoefficientDraft((current) => (current ? updater(current) : current));
  };

  const confirmParameterDraft = () => {
    if (!parameterDraft) {
      return;
    }
    updateParameterRow(parameterDraft.param_name, (item) => ({
      ...item,
      enabled: true,
      minimum: parameterDraft.minimum ?? null,
      maximum: parameterDraft.maximum ?? null,
      step: parameterDraft.step ?? null,
      level_count: parameterDraft.level_count ?? null,
    }));
    setParameterDraft(null);
    setExpandedParameterKey(null);
  };

  const confirmCoefficientDraft = () => {
    if (!coefficientDraft) {
      return;
    }
    const rowKey = `${coefficientDraft.function_name}-${coefficientDraft.x_name}-${coefficientDraft.beta_name}`;
    updateCoefficientRow(rowKey, (item) => ({
      ...item,
      enabled: true,
      minimum: coefficientDraft.minimum ?? null,
      maximum: coefficientDraft.maximum ?? null,
      step: coefficientDraft.step ?? null,
      level_count: coefficientDraft.level_count ?? null,
    }));
    setCoefficientDraft(null);
    setExpandedCoefficientKey(null);
  };

  const renderParameterDraft = () => {
    if (!parameterDraft) {
      return null;
    }
    return (
      <div className="profile-draft-section">
        <h4>Pending Add</h4>
        <div className="profile-selected-card profile-draft-card">
          <div className="profile-selected-summary-row">
            <div className="profile-selected-summary-copy">
              <div className="profile-selected-summary-copy-inline">
                <strong>{parameterDraft.param_name}</strong>
                <span className="profile-selected-summary-text">
                  {buildSelectedSummary({
                    defaultValue: parameterDraft.param_default,
                    minimum: parameterDraft.minimum,
                    maximum: parameterDraft.maximum,
                    step: parameterDraft.step,
                    levelCount: parameterDraft.level_count,
                  })}
                </span>
              </div>
              <span className="profile-selected-summary-text">This parameter will appear in the selected summary after you confirm Add.</span>
            </div>
            <div className="profile-selected-actions">
              <button className="button" type="button" onClick={confirmParameterDraft}>
                Add
              </button>
              <button className="button secondary profile-small-button" type="button" onClick={() => setParameterDraft(null)}>
                Cancel
              </button>
            </div>
          </div>
          <div className="profile-selected-grid">
            <label><span>Minimum</span><DraftNumberInput value={parameterDraft.minimum} onValueChange={(value) => updateParameterDraft((item) => ({ ...item, minimum: value }))} /></label>
            <label><span>Maximum</span><DraftNumberInput value={parameterDraft.maximum} onValueChange={(value) => updateParameterDraft((item) => ({ ...item, maximum: value }))} /></label>
            <label><span>Step</span><DraftNumberInput value={parameterDraft.step} onValueChange={(value) => updateParameterDraft((item) => ({ ...item, step: value }))} /></label>
            <label><span>Levels</span><DraftNumberInput integer value={parameterDraft.level_count} onValueChange={(value) => updateParameterDraft((item) => ({ ...item, level_count: value == null ? null : Math.trunc(value) }))} /></label>
          </div>
        </div>
      </div>
    );
  };

  const renderCoefficientDraft = () => {
    if (!coefficientDraft) {
      return null;
    }
    return (
      <div className="profile-draft-section">
        <h4>Pending Add</h4>
        <div className="profile-selected-card profile-draft-card">
          <div className="profile-selected-summary-row">
            <div className="profile-selected-summary-copy">
              <div className="profile-selected-summary-copy-inline">
                <strong>{coefficientDraft.function_name}</strong>
                <span className="profile-selected-summary-text">
                  {[`${coefficientDraft.x_name} / ${coefficientDraft.beta_name}`, buildSelectedSummary({
                    defaultValue: coefficientDraft.beta_default,
                    minimum: coefficientDraft.minimum,
                    maximum: coefficientDraft.maximum,
                    step: coefficientDraft.step,
                    levelCount: coefficientDraft.level_count,
                  })].join(" | ")}
                </span>
              </div>
              <span className="profile-selected-summary-text">This coefficient will appear in the selected summary after you confirm Add.</span>
            </div>
            <div className="profile-selected-actions">
              <button className="button" type="button" onClick={confirmCoefficientDraft}>
                Add
              </button>
              <button className="button secondary profile-small-button" type="button" onClick={() => setCoefficientDraft(null)}>
                Cancel
              </button>
            </div>
          </div>
          <div className="profile-selected-grid">
            <label><span>Minimum</span><DraftNumberInput value={coefficientDraft.minimum} onValueChange={(value) => updateCoefficientDraft((item) => ({ ...item, minimum: value }))} /></label>
            <label><span>Maximum</span><DraftNumberInput value={coefficientDraft.maximum} onValueChange={(value) => updateCoefficientDraft((item) => ({ ...item, maximum: value }))} /></label>
            <label><span>Step</span><DraftNumberInput value={coefficientDraft.step} onValueChange={(value) => updateCoefficientDraft((item) => ({ ...item, step: value }))} /></label>
            <label><span>Levels</span><DraftNumberInput integer value={coefficientDraft.level_count} onValueChange={(value) => updateCoefficientDraft((item) => ({ ...item, level_count: value == null ? null : Math.trunc(value) }))} /></label>
          </div>
        </div>
      </div>
    );
  };

  const previousEditorTab = previousTab(activeTab);
  const nextEditorTabValue = nextTab(activeTab);

  const renderParameterEditor = () => (
    <div className="profile-editor-step">
      <div className="profile-editor-step-header">
        <div>
          <h3 className="modal-title">Parameters</h3>
          <p className="modal-subtitle">Select only the parameters you want to tune, then set their ranges below.</p>
        </div>
        <button className="button" type="button" onClick={() => void saveParameters()} disabled={busy || !detail}>
          {busy ? "Saving..." : "Save Parameters"}
        </button>
      </div>
      <div className="profile-split-panel">
        <section className="profile-pane-shell">
          <div className="profile-pane-header">
            <h4>Selected Parameters</h4>
          </div>
          <div className="profile-pane-scroll">
            <div className="profile-selected-list">
              {selectedParameters.length === 0 ? (
                <div className="profile-empty-selection">No parameters selected yet. Use search or browse the catalog below.</div>
              ) : (
                selectedParameters.map((row) => {
                  const rowKey = row.param_name;
                  const isExpanded = expandedParameterKey === rowKey;
                  return (
                    <div key={rowKey} className={`profile-selected-card ${isExpanded ? "profile-selected-card-expanded" : ""}`}>
                      <div className="profile-selected-summary-row">
                        <div className="profile-selected-summary-copy profile-selected-summary-copy-inline">
                          <strong>{row.param_name}</strong>
                          <span className="profile-selected-summary-text">
                            {buildSelectedSummary({
                              defaultValue: row.param_default,
                              minimum: row.minimum,
                              maximum: row.maximum,
                              step: row.step,
                              levelCount: row.level_count,
                            })}
                          </span>
                        </div>
                        <div className="profile-selected-actions">
                          <button className="button secondary profile-small-button" type="button" onClick={() => setExpandedParameterKey(rowKey)}>
                            Edit
                          </button>
                          <button
                            className="button secondary profile-small-button"
                            type="button"
                            onClick={() => {
                              if (expandedParameterKey === rowKey) {
                                setExpandedParameterKey(null);
                              }
                              updateParameterRow(rowKey, (item) => ({ ...item, enabled: false }));
                            }}
                          >
                            Remove
                          </button>
                        </div>
                      </div>
                      {isExpanded ? (
                        <div className="profile-selected-grid">
                          <label><span>Minimum</span><DraftNumberInput value={row.minimum} onValueChange={(value) => updateParameterRow(rowKey, (item) => ({ ...item, minimum: value }))} /></label>
                          <label><span>Maximum</span><DraftNumberInput value={row.maximum} onValueChange={(value) => updateParameterRow(rowKey, (item) => ({ ...item, maximum: value }))} /></label>
                          <label><span>Step</span><DraftNumberInput value={row.step} onValueChange={(value) => updateParameterRow(rowKey, (item) => ({ ...item, step: value }))} /></label>
                          <label><span>Levels</span><DraftNumberInput integer value={row.level_count} onValueChange={(value) => updateParameterRow(rowKey, (item) => ({ ...item, level_count: value == null ? null : Math.trunc(value) }))} /></label>
                        </div>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
            {renderParameterDraft()}
          </div>
        </section>
        <section className="profile-pane-shell">
          <div className="profile-pane-header">
            <h4>Browse Catalog</h4>
            <div className="profile-catalog-toolbar">
              <input className="path-input" value={parameterSearch} onChange={(event) => setParameterSearch(event.target.value)} placeholder="Search parameter catalog" />
              <button className="button secondary profile-small-button" type="button" onClick={() => setShowAllParameters((current) => !current)}>{showAllParameters ? "Hide Catalog" : "Browse Catalog"}</button>
            </div>
          </div>
          <div className="profile-pane-scroll">
            <div className="profile-catalog-section">
              {filteredParameterCatalog.length === 0 ? renderCatalogEmptyState("parameters") : (
                <div className="profile-catalog-list">
                  {filteredParameterCatalog.slice(0, 40).map((row) => {
                    return (
                      <div key={row.param_name} className="profile-catalog-item">
                        <div><strong>{row.param_name}</strong><span className="profile-catalog-detail">Default: {row.param_default ?? "N/A"}</span></div>
                        <button
                          className="button secondary profile-small-button"
                          type="button"
                          onClick={() => {
                            setParameterDraft({
                              ...row,
                              enabled: false,
                              minimum: row.minimum ?? null,
                              maximum: row.maximum ?? null,
                              step: row.step ?? null,
                              level_count: row.level_count ?? null,
                            });
                          }}
                        >
                          Add
                        </button>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );

  const renderCoefficientEditor = () => (
    <div className="profile-editor-step">
      <div className="profile-editor-step-header">
        <div>
          <h3 className="modal-title">Coefficients</h3>
          <p className="modal-subtitle">Add the coefficient rows you want to tune, then edit their ranges.</p>
        </div>
        <button className="button" type="button" onClick={() => void saveCoefficients()} disabled={busy || !detail}>
          {busy ? "Saving..." : "Save Coefficients"}
        </button>
      </div>
      <div className="profile-split-panel">
        <section className="profile-pane-shell">
          <div className="profile-pane-header">
            <h4>Selected Coefficients</h4>
          </div>
          <div className="profile-pane-scroll">
            <div className="profile-selected-list">
              {selectedCoefficients.length === 0 ? (
                <div className="profile-empty-selection">No coefficients selected yet. Search or browse the catalog below.</div>
              ) : (
                selectedCoefficients.map((row) => {
                  const rowKey = `${row.function_name}-${row.x_name}-${row.beta_name}`;
                  const isExpanded = expandedCoefficientKey === rowKey;
                  return (
                    <div key={rowKey} className={`profile-selected-card ${isExpanded ? "profile-selected-card-expanded" : ""}`}>
                      <div className="profile-selected-summary-row">
                        <div className="profile-selected-summary-copy profile-selected-summary-copy-inline">
                          <strong>{row.function_name}</strong>
                          <span className="profile-selected-summary-text">
                            {[
                              `${row.x_name} / ${row.beta_name}`,
                              buildSelectedSummary({
                                defaultValue: row.beta_default,
                                minimum: row.minimum,
                                maximum: row.maximum,
                                step: row.step,
                                levelCount: row.level_count,
                              }),
                            ].join(" | ")}
                          </span>
                        </div>
                        <div className="profile-selected-actions">
                          <button className="button secondary profile-small-button" type="button" onClick={() => setExpandedCoefficientKey(rowKey)}>
                            Edit
                          </button>
                          <button
                            className="button secondary profile-small-button"
                            type="button"
                            onClick={() => {
                              if (expandedCoefficientKey === rowKey) {
                                setExpandedCoefficientKey(null);
                              }
                              updateCoefficientRow(rowKey, (item) => ({ ...item, enabled: false }));
                            }}
                          >
                            Remove
                          </button>
                        </div>
                      </div>
                      {isExpanded ? (
                        <div className="profile-selected-grid">
                          <label><span>Minimum</span><DraftNumberInput value={row.minimum} onValueChange={(value) => updateCoefficientRow(rowKey, (item) => ({ ...item, minimum: value }))} /></label>
                          <label><span>Maximum</span><DraftNumberInput value={row.maximum} onValueChange={(value) => updateCoefficientRow(rowKey, (item) => ({ ...item, maximum: value }))} /></label>
                          <label><span>Step</span><DraftNumberInput value={row.step} onValueChange={(value) => updateCoefficientRow(rowKey, (item) => ({ ...item, step: value }))} /></label>
                          <label><span>Levels</span><DraftNumberInput integer value={row.level_count} onValueChange={(value) => updateCoefficientRow(rowKey, (item) => ({ ...item, level_count: value == null ? null : Math.trunc(value) }))} /></label>
                        </div>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
            {renderCoefficientDraft()}
          </div>
        </section>
        <section className="profile-pane-shell">
          <div className="profile-pane-header">
            <h4>Browse Catalog</h4>
            <div className="profile-catalog-toolbar">
              <input className="path-input" value={coefficientSearch} onChange={(event) => setCoefficientSearch(event.target.value)} placeholder="Search coefficient catalog" />
              <button className="button secondary profile-small-button" type="button" onClick={() => setShowAllCoefficients((current) => !current)}>{showAllCoefficients ? "Hide Catalog" : "Browse Catalog"}</button>
            </div>
          </div>
          <div className="profile-pane-scroll">
            <div className="profile-catalog-section">
              {filteredCoefficientCatalog.length === 0 ? renderCatalogEmptyState("coefficients") : (
                <div className="profile-catalog-list">
                  {filteredCoefficientCatalog.slice(0, 40).map((row) => {
                    const rowKey = `${row.function_name}-${row.x_name}-${row.beta_name}`;
                    return (
                      <div key={rowKey} className="profile-catalog-item">
                        <div><strong>{row.function_name}</strong><span className="profile-catalog-detail">{row.x_name} / {row.beta_name}</span></div>
                        <button
                          className="button secondary profile-small-button"
                          type="button"
                          onClick={() => {
                            setCoefficientDraft({
                              ...row,
                              enabled: false,
                              minimum: row.minimum ?? null,
                              maximum: row.maximum ?? null,
                              step: row.step ?? null,
                              level_count: row.level_count ?? null,
                            });
                          }}
                        >
                          Add
                        </button>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );

  const renderOutputEditor = () => (
    <div className="profile-editor-step">
      <div className="profile-editor-step-header">
        <div>
          <h3 className="modal-title">Outputs</h3>
          <p className="modal-subtitle">Select the outputs you want model setup and ML optimization to track.</p>
        </div>
        <button className="button" type="button" onClick={() => void saveOutputs()} disabled={busy || !detail}>
          {busy ? "Saving..." : "Save Outputs"}
        </button>
      </div>
      <div className="profile-selected-list">
        <h4>Selected Outputs</h4>
        {selectedOutputs.length === 0 ? (
          <div className="profile-empty-selection">No outputs selected yet. Add at least one before running model setup.</div>
        ) : (
          selectedOutputs.map((row) => {
            const rowKey = `${row.variable}-${row.table_name}`;
            const rowIndex = outputRows.findIndex((item) => `${item.variable}-${item.table_name}` === rowKey);
            return (
              <div key={rowKey} className="profile-catalog-item profile-selected-output">
                <div><strong>{row.variable}</strong><span className="profile-catalog-detail">{row.table_name}</span></div>
                <button className="button secondary profile-small-button" type="button" onClick={() => setOutputRows((current) => current.map((item, itemIndex) => itemIndex === rowIndex ? { ...item, enabled: false } : item))}>Remove</button>
              </div>
            );
          })
        )}
      </div>
      <div className="profile-catalog-section">
        <div className="profile-catalog-toolbar">
          <input className="path-input" value={outputSearch} onChange={(event) => setOutputSearch(event.target.value)} placeholder="Search output catalog" />
          <button className="button secondary profile-small-button" type="button" onClick={() => setShowAllOutputs((current) => !current)}>{showAllOutputs ? "Hide Catalog" : "Browse Catalog"}</button>
        </div>
        {filteredOutputCatalog.length === 0 ? renderCatalogEmptyState("outputs") : (
          <div className="profile-catalog-list">
            {filteredOutputCatalog.slice(0, 40).map((row) => {
              const rowKey = `${row.variable}-${row.table_name}`;
              const rowIndex = outputRows.findIndex((item) => `${item.variable}-${item.table_name}` === rowKey);
              return (
                <div key={rowKey} className="profile-catalog-item">
                  <div><strong>{row.variable}</strong><span className="profile-catalog-detail">{row.table_name}</span></div>
                  <button className="button secondary profile-small-button" type="button" onClick={() => setOutputRows((current) => current.map((item, itemIndex) => itemIndex === rowIndex ? { ...item, enabled: true } : item))}>Add</button>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );

  const renderMlSettingsEditor = () => (
    <div className="profile-editor-step">
      <div className="profile-editor-step-header">
        <div>
          <h3 className="modal-title">ML Settings</h3>
          <p className="modal-subtitle">Set the runtime options that control the optimization process.</p>
        </div>
        <button className="button" type="button" onClick={() => void saveMlSettings()} disabled={busy || !detail || !mlSettings}>
          {busy ? "Saving..." : "Save ML Settings"}
        </button>
      </div>
      <div className="profile-ml-settings">
        <div className="input-row">
          <label className="profile-field-label">ML Method</label>
          <input className="path-input" value={mlSettings?.ml_method ?? ""} onChange={(event) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), ml_method: event.target.value }))} />
        </div>
        <div className="input-row">
          <label className="profile-field-label">Fit Metric</label>
          <input className="path-input" value={mlSettings?.fit_metric ?? ""} onChange={(event) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), fit_metric: event.target.value }))} />
        </div>
        <div className="profile-ml-grid">
          <label><span>n_sample</span><DraftNumberInput integer value={mlSettings?.n_sample} onValueChange={(value) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), n_sample: value == null ? 0 : Math.trunc(value) }))} /></label>
          <label><span>n_max_iteration</span><DraftNumberInput integer value={mlSettings?.n_max_iteration} onValueChange={(value) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), n_max_iteration: value == null ? 0 : Math.trunc(value) }))} /></label>
          <label><span>n_convergence</span><DraftNumberInput integer value={mlSettings?.n_convergence} onValueChange={(value) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), n_convergence: value == null ? 0 : Math.trunc(value) }))} /></label>
          <label><span>min_convergence_pct</span><DraftNumberInput value={mlSettings?.min_convergence_pct} onValueChange={(value) => setMLSettings((current) => ({ ...(current ?? detail?.ml_settings ?? DEFAULT_ML_SETTINGS), min_convergence_pct: value ?? 0 }))} /></label>
        </div>
      </div>
    </div>
  );

  const renderEditorBody = () => {
    switch (activeTab) {
      case "parameters":
        return renderParameterEditor();
      case "coefficients":
        return renderCoefficientEditor();
      case "outputs":
        return renderOutputEditor();
      case "ml":
        return renderMlSettingsEditor();
      default:
        return null;
    }
  };

  return (
    <section className="results profile-panel">
      {panelMode === "editor" && detail ? (
        <>
          <div className="profile-editor-topbar">
            <div>
              <h2>Profile Editor</h2>
              <p className="profile-editor-subtitle">
                {detail.profile.name}
                {detail.profile.description ? ` - ${detail.profile.description}` : ""}
              </p>
            </div>
            <div className="button-row multi">
              <button className="button secondary" type="button" onClick={openSaveAsModal} disabled={busy}>
                Save As
              </button>
              <button className="button secondary" type="button" onClick={handleEditorBack}>
                Back to Model Setup
              </button>
            </div>
          </div>
          {message ? <div className="alert alert-info">{message}</div> : null}
          {error ? <div className="alert alert-error">{error}</div> : null}
          <div className="summary">
            <div className={`summary-line ${detail.validation.valid ? "success" : "error"}`}>
              <span className="summary-label">Profile status:</span>
              <ValidationBadge valid={Boolean(detail.validation.valid)} />
              <span className="summary-message">{summaryText}</span>
            </div>
            {detail.validation.errors.length ? <div className="summary-line error"><span className="summary-label">Validation:</span><span className="summary-message">{detail.validation.errors.join(" ")}</span></div> : null}
          </div>
          <div className="profile-editor-meta">
            <div className="input-row">
              <label className="profile-field-label">Profile Name</label>
              <input className="path-input" value={metaName} onChange={(event) => setMetaName(event.target.value)} />
            </div>
            <div className="input-row">
              <label className="profile-field-label">Description</label>
              <input className="path-input" value={metaDescription} onChange={(event) => setMetaDescription(event.target.value)} />
            </div>
            <div className="button-row multi">
              <button className="button" type="button" onClick={() => void handleSaveMeta()} disabled={busy}>
                {busy ? "Saving..." : "Save Details"}
              </button>
              <button className="button secondary" type="button" onClick={() => void loadProfileDetail(detail.profile.profile_id)} disabled={loadingDetail}>
                {loadingDetail ? "Refreshing..." : "Refresh Profile"}
              </button>
            </div>
          </div>
          <div className="profile-editor-steps">
            {PROFILE_TABS.map((tab) => (
              <button key={tab} type="button" className={`button profile-tab-button ${activeTab === tab ? "profile-tab-button-active" : ""}`} onClick={() => setActiveTab(tab)}>
                {tab === "ml" ? "ML Settings" : tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>
          {renderEditorBody()}
          <div className="profile-editor-nav">
            <button className="button secondary" type="button" onClick={() => previousEditorTab && setActiveTab(previousEditorTab)} disabled={!previousEditorTab}>Previous Step</button>
            <button className="button secondary" type="button" onClick={() => nextEditorTabValue && setActiveTab(nextEditorTabValue)} disabled={!nextEditorTabValue}>Next Step</button>
          </div>
        </>
      ) : (
        <>
          <h2>Input Profiles</h2>
          {!canLoadProfiles ? <div className="alert alert-info">Validate a readable IFs folder and writable output folder to load profile storage.</div> : null}
          {message ? <div className="alert alert-info">{message}</div> : null}
          {error ? <div className="alert alert-error">{error}</div> : null}
          <div className="summary">
            <div className={`summary-line ${detail?.validation.valid ? "success" : "error"}`}>
              <span className="summary-label">Profile status:</span>
              <ValidationBadge valid={Boolean(detail?.validation.valid)} />
              <span className="summary-message">{summaryText}</span>
            </div>
            {!detail ? <div className="summary-line profile-guidance-line"><span className="summary-label">Getting started:</span><span className="summary-message">Create or open an input profile, then edit it in the dedicated editor before running model setup.</span></div> : null}
            {isBlankProfile ? <div className="summary-line profile-guidance-line"><span className="summary-label">Next steps:</span><span className="summary-message">Open the profile editor, select parameters, add outputs, and save each step before running model setup.</span></div> : null}
            {detail?.validation.errors?.length ? <div className="summary-line error"><span className="summary-label">Validation:</span><span className="summary-message">{detail.validation.errors.join(" ")}</span></div> : null}
          </div>
          <div className="input-row">
            <label className="profile-field-label">Saved Profiles</label>
            <select className="path-input" value={selectedProfileId ?? ""} onChange={(event) => onSelectedProfileIdChange(event.target.value ? Number(event.target.value) : null)} disabled={!profiles.length || loadingProfiles}>
              <option value="">Select a profile</option>
              {profiles.map((profile) => <option key={profile.profile_id} value={profile.profile_id}>{profile.name}</option>)}
            </select>
          </div>
          <div className="button-row multi profile-create-actions">
            <button className="button" type="button" onClick={openCreateModal} disabled={!canLoadProfiles || busy}>Create Input Profile</button>
            {selectedProfileId != null ? (
              <button
                className="button"
                type="button"
                onClick={() => void handleOpenEditor()}
                disabled={!detail || busy || loadingDetail}
              >
                {loadingDetail ? "Loading..." : "Open Editor"}
              </button>
            ) : null}
          </div>
          {!detail ? (
            <div className="alert alert-info profile-empty-state">
              <p className="alert-message">No input profile is open yet. Create one or select an existing profile to continue.</p>
            </div>
          ) : (
            <>
              <div className="profile-summary-grid">
                <SummaryCard title="Profile" value={detail.profile.name} detail={detail.profile.description ?? "No description"} />
                <SummaryCard title="Parameters" value={detail.validation.enabled_param_count} detail="Enabled for tuning" />
                <SummaryCard title="Coefficients" value={detail.validation.enabled_coefficient_count} detail="Enabled for tuning" />
                <SummaryCard title="Outputs" value={detail.validation.enabled_output_count} detail="Selected outputs" />
                <SummaryCard title="ML Method" value={detail.ml_settings.ml_method || "Not set"} detail={`Fit metric: ${detail.ml_settings.fit_metric}`} />
                <SummaryCard title="Iterations" value={detail.ml_settings.n_max_iteration} detail={`Samples: ${detail.ml_settings.n_sample}`} />
              </div>
              <div className="button-row multi">
                <button className="button" type="button" onClick={() => void handleOpenEditor()} disabled={busy || loadingDetail}>
                  {loadingDetail ? "Loading..." : "Open Editor"}
                </button>
                <button className="button button-danger" type="button" onClick={() => void handleDelete()} disabled={busy}>
                  Delete Profile
                </button>
              </div>
            </>
          )}
        </>
      )}
      {isCreateModalOpen ? (
        <div className="modal-backdrop" onClick={closeCreateModal} role="presentation">
          <div className="modal-content profile-create-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label="Create input profile">
            <form className="profile-create-form" onSubmit={(event) => {
              event.preventDefault();
              void handleCreate();
            }}>
              <div className="profile-create-modal-header">
                <div>
                  <h3 className="modal-title">Create Input Profile</h3>
                  <p className="modal-subtitle">The new profile will open directly in the dedicated editor so you can start with parameters.</p>
                </div>
              </div>
              {error ? <div className="alert alert-error profile-create-alert">{error}</div> : null}
              <div className="input-row">
                <label className="profile-field-label" htmlFor="profile-create-name">Profile Name</label>
                <input id="profile-create-name" className="path-input" value={createName} onChange={(event) => setCreateName(event.target.value)} placeholder="Profile name" autoFocus />
              </div>
              <div className="input-row">
                <label className="profile-field-label" htmlFor="profile-create-description">Description</label>
                <input id="profile-create-description" className="path-input" value={createDescription} onChange={(event) => setCreateDescription(event.target.value)} placeholder="Optional description" />
              </div>
              <div className="summary">
                <div className="summary-line profile-guidance-line">
                  <span className="summary-label">What happens next:</span>
                  <span className="summary-message">The editor will open on the Parameters step. You can save each step explicitly and return to model setup when the profile is ready.</span>
                </div>
              </div>
              <div className="button-row multi profile-create-modal-actions">
                <button className="button secondary" type="button" onClick={closeCreateModal} disabled={busy}>Cancel</button>
                <button className="button" type="submit" disabled={busy || !canLoadProfiles}>{busy ? "Creating..." : "Create Blank Profile"}</button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
      {isSaveAsModalOpen && detail ? (
        <div className="modal-backdrop" onClick={closeSaveAsModal} role="presentation">
          <div className="modal-content profile-create-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label="Save profile as">
            <form
              className="profile-create-form"
              onSubmit={(event) => {
                event.preventDefault();
                void handleSaveAs();
              }}
            >
              <div className="profile-create-modal-header">
                <div>
                  <h3 className="modal-title">Save As</h3>
                  <p className="modal-subtitle">
                    Create a new profile from the current editor state without changing the original profile.
                  </p>
                </div>
              </div>
              {error ? <div className="alert alert-error profile-create-alert">{error}</div> : null}
              <div className="input-row">
                <label className="profile-field-label" htmlFor="profile-save-as-name">Profile Name</label>
                <input
                  id="profile-save-as-name"
                  className="path-input"
                  value={saveAsName}
                  onChange={(event) => setSaveAsName(event.target.value)}
                  placeholder="New profile name"
                  autoFocus
                />
              </div>
              <div className="input-row">
                <label className="profile-field-label" htmlFor="profile-save-as-description">Description</label>
                <input
                  id="profile-save-as-description"
                  className="path-input"
                  value={saveAsDescription}
                  onChange={(event) => setSaveAsDescription(event.target.value)}
                  placeholder="Optional description"
                />
              </div>
              <div className="button-row multi profile-create-modal-actions">
                <button className="button secondary" type="button" onClick={closeSaveAsModal} disabled={busy}>
                  Cancel
                </button>
                <button className="button" type="submit" disabled={busy}>
                  {busy ? "Saving..." : "Save As"}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
    </section>
  );
}

export default InputProfilesPanel;
